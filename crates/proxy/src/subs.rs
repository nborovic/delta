//! Subscription manager. Each upstream holds at most one subscription per
//! `(method, canonical params)`, however many clients or routes ask for it.
//! A `single` client subscription runs on one upstream at a time and moves
//! to another when it drops or fails; a `redundant` one runs on every
//! upstream of its method, and the first copy of each notification is
//! forwarded.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use delta_engine::health::Signal;
use delta_engine::jsonrpc::{self, canonical_json, Answer, RpcError};
use delta_engine::policy::{EffectivePolicies, Mode};
use delta_engine::{solana, Shared, UpstreamId};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::Message;

use crate::auth::KeyId;

/// How long a subscribe waits for the first upstream to confirm.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Notifications an event-stream subscription may have queued before the
/// client is closed.
const EVENT_BUFFER: usize = 1024;
/// Recent notifications remembered per client subscription for dedup.
/// Slots of recent notification ids remembered per client subscription for
/// dedup (about 25 s of mainnet slots).
const DEDUP_SLOTS: u64 = 64;
/// Ids remembered for notifications that carry no slot.
const DEDUP_UNSLOTTED: usize = 4096;
const PING_INTERVAL: Duration = Duration::from_secs(20);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Minimum wait between reconnect attempts, on top of the health backoff.
const RECONNECT_FLOOR: Duration = Duration::from_secs(1);
/// An upstream that refused a subscription still in use is asked again
/// after this, doubling each time up to `RESUBSCRIBE_CAP`.
const RESUBSCRIBE_BASE: Duration = Duration::from_secs(1);
const RESUBSCRIBE_CAP: Duration = Duration::from_secs(30);

/// One client WebSocket connection, as the manager sees it.
pub struct ConnHandle {
    pub id: u64,
    tx: mpsc::UnboundedSender<Outgoing>,
    close: watch::Sender<Option<String>>,
}

/// What the connection's writer sends next.
pub enum Outgoing {
    Text(String),
    /// A response to a client message; the permit is its in-flight slot,
    /// freed once the response is written.
    Response(String, tokio::sync::OwnedSemaphorePermit),
    /// The newest state of a state subscription.
    Latest(Arc<LatestSlot>),
    /// One event of an event-stream subscription; the counter is decremented
    /// once written.
    Event(Arc<AtomicUsize>, String),
}

#[derive(Default)]
pub struct LatestSlot {
    value: Mutex<Option<String>>,
    queued: AtomicBool,
}

impl LatestSlot {
    pub fn take(&self) -> Option<String> {
        self.queued.store(false, Ordering::SeqCst);
        self.value.lock().unwrap().take()
    }
}

impl ConnHandle {
    pub fn new(id: u64) -> (Arc<Self>, mpsc::UnboundedReceiver<Outgoing>, watch::Receiver<Option<String>>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (close, close_rx) = watch::channel(None);
        (Arc::new(Self { id, tx, close }), rx, close_rx)
    }

    pub fn send(&self, text: String) {
        let _ = self.tx.send(Outgoing::Text(text));
    }

    /// Queues a response; `permit` is released once it has been written.
    pub fn respond(&self, text: String, permit: tokio::sync::OwnedSemaphorePermit) {
        let _ = self.tx.send(Outgoing::Response(text, permit));
    }

    /// Closes the client connection so it knows it missed data.
    pub fn close(&self, reason: &str) {
        let _ = self.close.send(Some(reason.to_owned()));
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct SubKey {
    method: String,
    hash: [u8; 32],
}

impl SubKey {
    fn new(method: &str, params: Option<&Value>) -> Self {
        SubKey { method: method.to_owned(), hash: *solana::params_hash(params).as_bytes() }
    }
}

#[derive(Clone, Debug)]
enum UpStatus {
    Pending,
    Active,
    Failed(RpcError),
    Down,
}

struct UpEntry {
    refs: usize,
    status: UpStatus,
    /// Refusals in a row, for the resubscribe backoff.
    refusals: u32,
}

struct KeyEntry {
    method: String,
    ups: HashMap<UpstreamId, UpEntry>,
    clients: HashSet<u64>,
}

struct ClientSub {
    conn: Arc<ConnHandle>,
    /// The API key that opened it, for the per-key limit.
    owner: KeyId,
    key: SubKey,
    method: String,
    params: Option<Value>,
    /// The method's policies: its mode and the selector over its upstreams.
    plan: Arc<EffectivePolicies>,
    /// The upstreams it runs on now: one for `single`, all for `redundant`.
    upstreams: Vec<UpstreamId>,
    /// Upstreams a `single` subscription failed on since it last ran; not
    /// tried again until it runs somewhere.
    tried: Vec<UpstreamId>,
    waiter: Option<oneshot::Sender<Result<(), Answer>>>,
    /// Every upstream of this subscription was down at some point: data may
    /// have been missed.
    gap: bool,
    dedup: Dedup,
    /// Its subscribe response has been queued to the client. Until then its
    /// notifications are held: a client cannot match a notification to a
    /// subscription id it has not received yet.
    acked: bool,
    /// Notifications held until `acked`.
    held: Vec<String>,
    /// A final signature notification arrived before `acked`: end the
    /// subscription once it is delivered.
    finish_on_ack: bool,
    latest: Arc<LatestSlot>,
    queued_events: Arc<AtomicUsize>,
    /// Hash of the last `value` forwarded (account subscriptions), for gap
    /// recovery.
    last_value: Option<blake3::Hash>,
}

/// Which notifications reach a client subscription fed by several upstreams.
///
/// Every subscription: an identical notification is sent once (ids are
/// remembered per slot, for the last `DEDUP_SLOTS` slots).
/// State subscriptions only: never a notification for an older slot than one
/// already sent, since it would replace newer state with older. Event
/// streams take every unseen event, however late: each one stands alone.
#[derive(Default)]
struct Dedup {
    state: bool,
    newest: Option<u64>,
    by_slot: BTreeMap<u64, HashSet<u64>>,
    unslotted: VecDeque<u64>,
    unslotted_ids: HashSet<u64>,
}

impl Dedup {
    fn new(state: bool) -> Self {
        Dedup { state, ..Dedup::default() }
    }

    fn accept(&mut self, slot: Option<u64>, hash: blake3::Hash) -> bool {
        let id = u64::from_le_bytes(hash.as_bytes()[..8].try_into().expect("8 bytes"));
        let Some(slot) = slot else {
            if !self.unslotted_ids.insert(id) {
                return false;
            }
            self.unslotted.push_back(id);
            if self.unslotted.len() > DEDUP_UNSLOTTED {
                if let Some(old) = self.unslotted.pop_front() {
                    self.unslotted_ids.remove(&old);
                }
            }
            return true;
        };
        if self.state && self.newest.is_some_and(|n| slot < n) {
            return false;
        }
        if !self.by_slot.entry(slot).or_default().insert(id) {
            return false;
        }
        let newest = self.newest.map_or(slot, |n| n.max(slot));
        self.newest = Some(newest);
        self.by_slot = self.by_slot.split_off(&newest.saturating_sub(DEDUP_SLOTS));
        true
    }
}

#[derive(Default)]
struct State {
    keys: HashMap<SubKey, KeyEntry>,
    clients: HashMap<u64, ClientSub>,
    /// Open client subscriptions per API key.
    per_key: HashMap<KeyId, usize>,
}

/// The WebSocket connections to one upstream. Each subscription lives on
/// one of them; a new one is opened when every connection holds the
/// upstream's `maxSubscriptionsPerConnection`.
#[derive(Default)]
struct UpstreamConns {
    conns: Vec<Conn>,
    of_key: HashMap<SubKey, usize>,
}

struct Conn {
    tx: mpsc::UnboundedSender<Cmd>,
    subscriptions: usize,
}

/// Commands to an upstream connection task.
enum Cmd {
    Want { key: SubKey, method: String, params: Option<Value> },
    Unwant { key: SubKey },
}

pub struct Subscriptions {
    shared: Arc<Shared>,
    state: Mutex<State>,
    actors: Mutex<HashMap<UpstreamId, UpstreamConns>>,
    next_id: AtomicU64,
    me: Weak<Subscriptions>,
}

impl Subscriptions {
    pub fn new(shared: Arc<Shared>) -> Arc<Self> {
        Arc::new_cyclic(|me| Subscriptions {
            shared,
            state: Mutex::new(State::default()),
            actors: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            me: me.clone(),
        })
    }

    /// Opens (or attaches to) a subscription and returns the proxy-issued
    /// id: on one upstream for `single` (trying the next when it fails), on
    /// every one of the method's upstreams for `redundant`. `limit` is the
    /// owner key's `maxSubscriptions`.
    pub async fn subscribe(
        &self,
        conn: &Arc<ConnHandle>,
        plan: &Arc<EffectivePolicies>,
        method: &str,
        params: Option<Value>,
        owner: KeyId,
        limit: Option<usize>,
    ) -> Answer {
        let key = SubKey::new(method, params.as_ref());
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut st = self.state.lock().unwrap();
            let held = st.per_key.get(&owner).copied().unwrap_or(0);
            if let Some(max) = limit.filter(|&m| held >= m) {
                return Answer::error(
                    jsonrpc::SUBSCRIPTION_LIMIT,
                    format!("subscription limit reached: this API key may hold {max} subscriptions"),
                );
            }
            *st.per_key.entry(owner).or_default() += 1;
            let entry = st.keys.entry(key.clone()).or_insert_with(|| KeyEntry {
                method: method.to_owned(),
                ups: HashMap::new(),
                clients: HashSet::new(),
            });
            entry.clients.insert(id);
            st.clients.insert(
                id,
                ClientSub {
                    conn: conn.clone(),
                    owner,
                    key: key.clone(),
                    method: method.to_owned(),
                    params,
                    plan: plan.clone(),
                    upstreams: Vec::new(),
                    tried: Vec::new(),
                    waiter: None,
                    gap: false,
                    dedup: Dedup::new(solana::is_state_subscription(method)),
                    acked: false,
                    held: Vec::new(),
                    finish_on_ack: false,
                    latest: Arc::new(LatestSlot::default()),
                    queued_events: Arc::new(AtomicUsize::new(0)),
                    last_value: None,
                },
            );
        }
        let single = plan.mode != Mode::Redundant;
        let deadline = tokio::time::Instant::now() + SUBSCRIBE_TIMEOUT;
        let mut ups = if single {
            self.pick_single(&key, plan, &[]).into_iter().collect()
        } else {
            plan.selector.candidates().to_vec()
        };
        let mut tried = Vec::new();
        loop {
            tried.extend_from_slice(&ups);
            let (tx, rx) = oneshot::channel();
            let outcome = match self.attach(id, &ups, Some(tx)) {
                Some(r) => r,
                None => match tokio::time::timeout_at(deadline, rx).await {
                    Ok(Ok(r)) => r,
                    _ => Err(Answer::error(jsonrpc::UPSTREAM_FAILED, "upstream failed: subscribe timed out")),
                },
            };
            match outcome {
                Ok(()) => return Answer::Result(id.to_string().into()),
                Err(answer) => {
                    let next = single && tokio::time::Instant::now() < deadline;
                    if let Some(up) = next.then(|| self.pick_single(&key, plan, &tried)).flatten() {
                        ups = vec![up];
                        continue;
                    }
                    self.detach(id);
                    return answer;
                }
            }
        }
    }

    /// The upstream for a `single` subscription: one already holding the
    /// same subscription (so identical subscriptions keep sharing it), else
    /// the method's `loadBalancing` pick. `None` once every upstream is in
    /// `exclude`.
    fn pick_single(&self, key: &SubKey, plan: &EffectivePolicies, exclude: &[UpstreamId]) -> Option<UpstreamId> {
        let candidates = plan.selector.candidates().iter().filter(|u| !exclude.contains(u));
        {
            let st = self.state.lock().unwrap();
            if let Some(entry) = st.keys.get(key) {
                let holds = |up: &UpstreamId| {
                    entry.ups.get(up).is_some_and(|e| matches!(e.status, UpStatus::Active | UpStatus::Pending))
                };
                if let Some(&up) = candidates.clone().find(|u| holds(u)) {
                    return Some(up);
                }
            }
        }
        plan.selector.pick(&self.shared, &key.method, 1, exclude).first().copied()
    }

    /// Puts client subscription `id` on `ups`, in place of the upstreams it
    /// ran on (a `single` one moving to another upstream). Returns the
    /// outcome when it is already known; otherwise `waiter` hears it.
    fn attach(&self, id: u64, ups: &[UpstreamId], waiter: Option<oneshot::Sender<Result<(), Answer>>>) -> Option<Result<(), Answer>> {
        let gone = || Some(Err(Answer::error(jsonrpc::UPSTREAM_FAILED, "upstream failed: subscription closed")));
        let (key, method, params, wants, unwants, immediate, recover) = {
            let mut guard = self.state.lock().unwrap();
            let st = &mut *guard;
            let Some(client) = st.clients.get_mut(&id) else { return gone() };
            let Some(entry) = st.keys.get_mut(&client.key) else { return gone() };
            let old = std::mem::replace(&mut client.upstreams, ups.to_vec());
            let mut unwants = Vec::new();
            for up in old.iter().filter(|u| !ups.contains(u)) {
                if let Some(e) = entry.ups.get_mut(up) {
                    e.refs -= 1;
                    if e.refs == 0 {
                        entry.ups.remove(up);
                        unwants.push(*up);
                    }
                }
            }
            let mut wants = Vec::new();
            for &up in ups.iter().filter(|u| !old.contains(u)) {
                let e = entry.ups.entry(up).or_insert(UpEntry { refs: 0, status: UpStatus::Pending, refusals: 0 });
                e.refs += 1;
                if e.refs == 1 {
                    e.status = UpStatus::Pending;
                    wants.push(up);
                }
            }
            let statuses: Vec<UpStatus> = ups.iter().map(|u| entry.ups[u].status.clone()).collect();
            let immediate = settle(&statuses);
            if immediate.is_none() {
                client.waiter = waiter;
            }
            // Moved onto a subscription already running: no confirmation
            // will come to trigger gap recovery, so start it here.
            let recover = client.gap && client.waiter.is_none() && matches!(immediate, Some(Ok(())));
            if recover {
                client.gap = false;
                client.tried.clear();
            }
            (client.key.clone(), client.method.clone(), client.params.clone(), wants, unwants, immediate, recover)
        };
        for up in unwants {
            self.command(up, Cmd::Unwant { key: key.clone() });
        }
        for &up in &wants {
            let placed = self.command(up, Cmd::Want { key: key.clone(), method: method.clone(), params: params.clone() });
            if !placed {
                let full = RpcError::new(jsonrpc::SUBSCRIPTION_LIMIT, "subscription limit reached: no upstream connection has room");
                self.on_confirmed(up, &key, Err(full));
            }
        }
        if recover {
            if let (Some(me), Some(&up)) = (self.me.upgrade(), ups.first()) {
                tokio::spawn(async move { me.recover_gap(id, up).await });
            }
        }
        immediate
    }

    /// `single` subscriptions whose upstream dropped or refused them move to
    /// another upstream; gap recovery follows once they run there. With no
    /// other upstream left, one whose upstream dropped waits for it to
    /// reconnect; one refused everywhere closes the client.
    fn failover(&self, moves: Vec<(u64, UpstreamId)>) {
        for (cid, from) in moves {
            let found = {
                let mut st = self.state.lock().unwrap();
                let Some(c) = st.clients.get_mut(&cid) else { continue };
                if c.upstreams != [from] {
                    continue;
                }
                if !c.tried.contains(&from) {
                    c.tried.push(from);
                }
                (c.key.clone(), c.plan.clone(), c.tried.clone())
            };
            let (key, plan, tried) = found;
            match self.pick_single(&key, &plan, &tried) {
                Some(up) => {
                    tracing::info!(method = %key.method, from = %self.shared.upstreams[from].name, to = %self.shared.upstreams[up].name, "subscription moved");
                    self.attach(cid, &[up], None);
                }
                None => {
                    let st = self.state.lock().unwrap();
                    let refused = st.keys.get(&key).and_then(|e| e.ups.get(&from)).is_some_and(|e| matches!(e.status, UpStatus::Failed(_)));
                    if let (true, Some(c)) = (refused, st.clients.get(&cid)) {
                        c.conn.close(&format!("{} failed: no upstream took the subscription", key.method));
                    }
                }
            }
        }
    }

    /// The subscribe responses for these ids have been queued to the client:
    /// release the notifications held for them, in order.
    pub fn ack(&self, ids: &[u64]) {
        let mut finished = Vec::new();
        {
            let mut st = self.state.lock().unwrap();
            for &id in ids {
                let Some(c) = st.clients.get_mut(&id) else { continue };
                c.acked = true;
                for raw in std::mem::take(&mut c.held) {
                    deliver(c, id, &raw);
                }
                if c.finish_on_ack {
                    finished.push(id);
                }
            }
        }
        for id in finished {
            self.detach(id);
        }
    }

    /// `<x>Unsubscribe [id]`: detaches the client's subscription.
    pub fn unsubscribe(&self, conn: &ConnHandle, method: &str, params: Option<&Value>) -> Answer {
        let id = params.and_then(|p| p.get(0)).and_then(Value::as_u64);
        let expected = solana::subscription_ended_by(method).map(|s| s.subscribe);
        let owned = id.is_some_and(|id| {
            let st = self.state.lock().unwrap();
            st.clients
                .get(&id)
                .is_some_and(|c| c.conn.id == conn.id && Some(c.method.as_str()) == expected)
        });
        match id {
            Some(id) if owned => {
                self.detach(id);
                Answer::Result("true".into())
            }
            _ => Answer::error(jsonrpc::INVALID_PARAMS, "Invalid subscription id."),
        }
    }

    /// Drops every subscription of a closed client connection.
    pub fn detach_conn(&self, conn_id: u64) {
        let ids: Vec<u64> = {
            let st = self.state.lock().unwrap();
            st.clients.iter().filter(|(_, c)| c.conn.id == conn_id).map(|(id, _)| *id).collect()
        };
        for id in ids {
            self.detach(id);
        }
    }

    /// Removes a client subscription; the last one on an upstream closes the
    /// upstream subscription (reference counting).
    fn detach(&self, id: u64) {
        let mut unwants = Vec::new();
        {
            let mut st = self.state.lock().unwrap();
            let Some(client) = st.clients.remove(&id) else { return };
            if let Some(n) = st.per_key.get_mut(&client.owner) {
                *n -= 1;
            }
            if let Some(entry) = st.keys.get_mut(&client.key) {
                entry.clients.remove(&id);
                for up in &client.upstreams {
                    if let Some(e) = entry.ups.get_mut(up) {
                        e.refs -= 1;
                        if e.refs == 0 {
                            entry.ups.remove(up);
                            unwants.push(*up);
                        }
                    }
                }
                if entry.clients.is_empty() {
                    st.keys.remove(&client.key);
                }
            }
            for up in unwants {
                self.command(up, Cmd::Unwant { key: client.key.clone() });
            }
        }
    }

    /// Sends a command to the connection that holds (or will hold) the
    /// subscription. False when a new subscription finds no room: every
    /// connection is full and `maxConnections` are open.
    fn command(&self, up: UpstreamId, cmd: Cmd) -> bool {
        let info = &self.shared.upstreams[up];
        let per_conn = info.max_subscriptions_per_connection.unwrap_or(usize::MAX);
        let max_conns = info.max_connections.unwrap_or(usize::MAX);
        let mut actors = self.actors.lock().unwrap();
        let ups = actors.entry(up).or_default();
        match &cmd {
            Cmd::Want { key, .. } => {
                let i = match ups.of_key.get(key) {
                    Some(&i) => i,
                    None => {
                        let i = match ups.conns.iter().position(|c| c.subscriptions < per_conn) {
                            Some(i) => i,
                            None if ups.conns.len() < max_conns => {
                                let (tx, rx) = mpsc::unbounded_channel();
                                let n = ups.conns.len();
                                tokio::spawn(run_upstream(self.me.clone(), self.shared.clone(), up, n, rx));
                                ups.conns.push(Conn { tx, subscriptions: 0 });
                                n
                            }
                            None => return false,
                        };
                        ups.conns[i].subscriptions += 1;
                        ups.of_key.insert(key.clone(), i);
                        i
                    }
                };
                let _ = ups.conns[i].tx.send(cmd);
            }
            Cmd::Unwant { key } => {
                if let Some(i) = ups.of_key.remove(key) {
                    ups.conns[i].subscriptions -= 1;
                    let _ = ups.conns[i].tx.send(cmd);
                }
            }
        }
        true
    }

    // ---- events from upstream connections ----

    fn on_confirmed(&self, up: UpstreamId, key: &SubKey, result: Result<(), RpcError>) {
        let mut recover = Vec::new();
        let mut moves = Vec::new();
        let retry_in;
        {
            let mut guard = self.state.lock().unwrap();
            let st = &mut *guard;
            let Some(entry) = st.keys.get_mut(key) else { return };
            let Some(ue) = entry.ups.get_mut(&up) else { return };
            ue.status = match &result {
                Ok(()) => UpStatus::Active,
                Err(e) => UpStatus::Failed(e.clone()),
            };
            // A refusal is asked again for as long as the subscription is
            // held there; `resubscribe` checks that it still is.
            retry_in = match &result {
                Ok(()) => {
                    ue.refusals = 0;
                    None
                }
                Err(_) => {
                    ue.refusals += 1;
                    Some((ue.refusals, RESUBSCRIBE_BASE.saturating_mul(1 << (ue.refusals - 1).min(5)).min(RESUBSCRIBE_CAP)))
                }
            };
            for cid in &entry.clients {
                let Some(client) = st.clients.get_mut(cid) else { continue };
                if !client.upstreams.contains(&up) {
                    continue;
                }
                let statuses: Vec<UpStatus> = client.upstreams.iter().map(|u| entry.ups[u].status.clone()).collect();
                if let Some(r) = settle(&statuses) {
                    if let Some(w) = client.waiter.take() {
                        let _ = w.send(r);
                        continue;
                    }
                }
                if result.is_ok() {
                    client.tried.clear();
                }
                if result.is_ok() && client.gap && client.waiter.is_none() {
                    client.gap = false;
                    recover.push((*cid, up));
                }
                if result.is_err() && client.waiter.is_none() && client.plan.mode != Mode::Redundant {
                    // Refused on resubscribe (after a reconnect or a move).
                    client.gap = true;
                    moves.push((*cid, up));
                }
            }
        }
        for (cid, up) in recover {
            if let Some(me) = self.me.upgrade() {
                tokio::spawn(async move { me.recover_gap(cid, up).await });
            }
        }
        self.failover(moves);
        if let (Some((refusals, delay)), Err(e)) = (retry_in, &result) {
            let name = &self.shared.upstreams[up].name;
            let text = format!("subscription refused ({}); asking again in {} s", e.message, delay.as_secs());
            // Once per run of refusals: an upstream that never takes it would
            // otherwise log every 30 s.
            if refusals == 1 {
                tracing::warn!(upstream = %name, method = %key.method, "{text}");
            } else {
                tracing::debug!(upstream = %name, method = %key.method, "{text}");
            }
            if let Some(me) = self.me.upgrade() {
                let key = key.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    me.resubscribe(up, &key);
                });
            }
        }
    }

    /// Asks `up` again for a subscription it refused, if any client still
    /// holds it there (a `redundant` one, or a `single` one with nowhere
    /// else to go).
    fn resubscribe(&self, up: UpstreamId, key: &SubKey) {
        let params = {
            let mut guard = self.state.lock().unwrap();
            let st = &mut *guard;
            let Some(entry) = st.keys.get_mut(key) else { return };
            let Some(ue) = entry.ups.get_mut(&up) else { return };
            if !matches!(ue.status, UpStatus::Failed(_)) {
                return;
            }
            let Some(params) = entry.clients.iter().find_map(|c| st.clients.get(c)).map(|c| c.params.clone()) else { return };
            ue.status = UpStatus::Pending;
            params
        };
        let placed = self.command(up, Cmd::Want { key: key.clone(), method: key.method.clone(), params });
        if !placed {
            let full = RpcError::new(jsonrpc::SUBSCRIPTION_LIMIT, "subscription limit reached: no upstream connection has room");
            self.on_confirmed(up, key, Err(full));
        }
    }

    /// One connection to `up` dropped: the subscriptions it carried are down.
    fn on_down(&self, up: UpstreamId, carried: &[SubKey]) {
        let mut moves = Vec::new();
        let mut guard = self.state.lock().unwrap();
        let st = &mut *guard;
        for k in carried {
            let Some(entry) = st.keys.get_mut(k) else { continue };
            let Some(ue) = entry.ups.get_mut(&up) else { continue };
            if matches!(ue.status, UpStatus::Active | UpStatus::Pending) {
                ue.status = UpStatus::Down;
            }
            for cid in &entry.clients {
                let Some(client) = st.clients.get_mut(cid) else { continue };
                if !client.upstreams.contains(&up) {
                    continue;
                }
                let statuses: Vec<UpStatus> = client.upstreams.iter().map(|u| entry.ups[u].status.clone()).collect();
                if client.waiter.is_some() {
                    if let Some(r) = settle(&statuses) {
                        let _ = client.waiter.take().unwrap().send(r);
                    }
                } else if !statuses.iter().any(|s| matches!(s, UpStatus::Active)) {
                    client.gap = true;
                    if client.plan.mode != Mode::Redundant {
                        moves.push((*cid, up));
                    }
                }
            }
        }
        drop(guard);
        self.failover(moves);
    }

    fn on_notification(&self, up: UpstreamId, key: &SubKey, result: &Value) {
        let mut finished = Vec::new();
        {
            let mut guard = self.state.lock().unwrap();
            let st = &mut *guard;
            let Some(entry) = st.keys.get(key) else { return };
            let raw = result.to_string();
            let slot = solana::notification_slot(result);
            let hash = solana::quorum_hash(&entry.method, raw.as_bytes());
            let final_signature =
                entry.method == "signatureSubscribe" && result.pointer("/value").and_then(Value::as_str) != Some("receivedSignature");
            let value_hash = (entry.method == "accountSubscribe").then(|| value_hash(result));
            for cid in &entry.clients {
                let Some(client) = st.clients.get_mut(cid) else { continue };
                if !client.upstreams.contains(&up) || client.waiter.is_some() {
                    continue;
                }
                if !client.dedup.accept(slot, hash) {
                    continue;
                }
                if value_hash.is_some() {
                    client.last_value = value_hash;
                }
                if send_or_hold(client, *cid, &raw) && final_signature {
                    finished.push(*cid);
                } else if final_signature {
                    client.finish_on_ack = true;
                }
            }
        }
        for cid in finished {
            // The upstream ends a signature subscription after notifying.
            self.detach(cid);
        }
    }

    /// Gap recovery after every upstream of a subscription was down and one
    /// came back.
    async fn recover_gap(self: Arc<Self>, cid: u64, up: UpstreamId) {
        let (method, params, conn) = {
            let st = self.state.lock().unwrap();
            let Some(c) = st.clients.get(&cid) else { return };
            (c.method.clone(), c.params.clone(), c.conn.clone())
        };
        match method.as_str() {
            "accountSubscribe" => {
                // Without the current value the client cannot know what it
                // missed: close it rather than carry on silently.
                let fetched = match self.http_call(up, "getAccountInfo", params.as_ref()).await {
                    Some(Answer::Result(body)) => serde_json::from_slice::<Value>(&body).ok(),
                    _ => None,
                };
                let Some(result) = fetched else {
                    conn.close("accountSubscribe gap: the current value could not be fetched");
                    return;
                };
                let vh = value_hash(&result);
                let mut st = self.state.lock().unwrap();
                if let Some(c) = st.clients.get_mut(&cid) {
                    if c.last_value != Some(vh) {
                        c.last_value = Some(vh);
                        let raw = result.to_string();
                        if c.dedup.accept(solana::notification_slot(&result), blake3::hash(raw.as_bytes())) {
                            send_or_hold(c, cid, &raw);
                        }
                    }
                }
            }
            "signatureSubscribe" => {
                let Some(sig) = params.as_ref().and_then(|p| p.get(0)).cloned() else { return };
                let wanted = solana::commitment(params.as_ref()).unwrap_or("finalized").to_owned();
                let q = json!([[sig], {"searchTransactionHistory": true}]);
                let fetched = match self.http_call(up, "getSignatureStatuses", Some(&q)).await {
                    Some(Answer::Result(body)) => serde_json::from_slice::<Value>(&body).ok(),
                    _ => None,
                };
                let Some(result) = fetched else {
                    conn.close("signatureSubscribe gap: the signature status could not be fetched");
                    return;
                };
                let Some(status) = result.pointer("/value/0").filter(|s| !s.is_null()) else { return };
                let reached = status.get("confirmationStatus").and_then(Value::as_str).map(rank).unwrap_or(0);
                if reached < rank(&wanted) {
                    return;
                }
                let note = json!({
                    "context": result.get("context").cloned().unwrap_or(Value::Null),
                    "value": {"err": status.get("err").cloned().unwrap_or(Value::Null)},
                });
                let delivered = {
                    let mut st = self.state.lock().unwrap();
                    match st.clients.get_mut(&cid) {
                        Some(c) => {
                            let sent = send_or_hold(c, cid, &note.to_string());
                            if !sent {
                                c.finish_on_ack = true;
                            }
                            sent
                        }
                        None => false,
                    }
                };
                if delivered {
                    self.detach(cid);
                }
            }
            m if !solana::is_state_subscription(m) => {
                // Lost events cannot be rebuilt.
                conn.close(&format!("{m} gap: events may have been missed"));
            }
            _ => {}
        }
    }

    async fn http_call(&self, up: UpstreamId, method: &str, params: Option<&Value>) -> Option<Answer> {
        let body = jsonrpc::upstream_body(method, params);
        let reply = self
            .shared
            .transport
            .post(&self.shared.upstreams[up].http, body, self.shared.limits.per_call_timeout)
            .await
            .ok()?;
        jsonrpc::parse_upstream(&reply.body)
    }
}

fn rank(commitment: &str) -> u8 {
    match commitment {
        "processed" => 1,
        "confirmed" => 2,
        "finalized" => 3,
        _ => 3,
    }
}

fn value_hash(result: &Value) -> blake3::Hash {
    let mut out = Vec::new();
    canonical_json(result.get("value").unwrap_or(&Value::Null), &mut out);
    blake3::hash(&out)
}

/// `Some` once the subscribe has an outcome: any upstream active = success;
/// every upstream failed or down = the first upstream error, or -33095.
fn settle(statuses: &[UpStatus]) -> Option<Result<(), Answer>> {
    if statuses.iter().any(|s| matches!(s, UpStatus::Active)) {
        return Some(Ok(()));
    }
    if statuses.iter().any(|s| matches!(s, UpStatus::Pending)) {
        return None;
    }
    let err = statuses.iter().find_map(|s| match s {
        UpStatus::Failed(e) => Some(Answer::Error(e.clone())),
        _ => None,
    });
    Some(Err(err.unwrap_or_else(|| Answer::error(jsonrpc::UPSTREAM_FAILED, "upstream failed: no upstream connected"))))
}

/// Delivers a notification, or holds it until the subscription's response
/// has gone out. True when delivered.
fn send_or_hold(client: &mut ClientSub, id: u64, result_raw: &str) -> bool {
    if client.acked {
        deliver(client, id, result_raw);
        return true;
    }
    if solana::is_state_subscription(&client.method) {
        // Only the newest state matters.
        client.held.clear();
    }
    client.held.push(result_raw.to_owned());
    if client.held.len() > EVENT_BUFFER {
        client.conn.close(&format!("{} buffer overflow: events were dropped", client.method));
    }
    false
}

/// Queues a notification for a client subscription. State subscriptions keep
/// only the latest; event streams close the client on overflow.
fn deliver(client: &ClientSub, id: u64, result_raw: &str) {
    let text = format!(
        r#"{{"jsonrpc":"2.0","method":"{}","params":{{"result":{},"subscription":{}}}}}"#,
        solana::subscription(&client.method).map_or("", |s| s.notification),
        result_raw,
        id
    );
    if solana::is_state_subscription(&client.method) {
        *client.latest.value.lock().unwrap() = Some(text);
        if !client.latest.queued.swap(true, Ordering::SeqCst) {
            let _ = client.conn.tx.send(Outgoing::Latest(client.latest.clone()));
        }
    } else {
        let queued = client.queued_events.fetch_add(1, Ordering::SeqCst) + 1;
        if queued > EVENT_BUFFER {
            client.conn.close(&format!("{} buffer overflow: events were dropped", client.method));
            return;
        }
        let _ = client.conn.tx.send(Outgoing::Event(client.queued_events.clone(), text));
    }
}

// ---- upstream connections ----

/// One WebSocket connection per upstream, carrying every subscription the
/// proxy holds on it. Reconnects in the background following the upstream's
/// health backoff.
async fn run_upstream(
    subs: Weak<Subscriptions>,
    shared: Arc<Shared>,
    up: UpstreamId,
    conn: usize,
    mut rx: mpsc::UnboundedReceiver<Cmd>,
) {
    let name = shared.upstreams[up].name.clone();
    let url = shared.upstreams[up].ws.clone();
    let mut wanted: HashMap<SubKey, (String, Option<Value>)> = HashMap::new();
    let mut last_attempt: Option<Instant> = None;

    loop {
        // Idle: wait for something to subscribe to.
        while wanted.is_empty() {
            match rx.recv().await {
                Some(cmd) => apply_offline(&mut wanted, cmd),
                None => return,
            }
        }
        // Follow the health backoff, with a floor between attempts.
        let mut ready = shared.health.upstream_ready_at(up);
        if let Some(t) = last_attempt {
            ready = ready.max(t + RECONNECT_FLOOR);
        }
        let sleep = tokio::time::sleep_until(ready.into());
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => break,
                cmd = rx.recv() => match cmd {
                    Some(cmd) => apply_offline(&mut wanted, cmd),
                    None => return,
                },
            }
        }
        if wanted.is_empty() {
            continue;
        }
        last_attempt = Some(Instant::now());

        let ws = match tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url.as_str())).await {
            Ok(Ok((ws, _))) => ws,
            Ok(Err(e)) => {
                tracing::debug!(upstream = %name, "websocket connect failed: {}", strip_url(&e.to_string()));
                shared.health.record_upstream(up, Signal::Failure("websocket connect".into()));
                if let Some(s) = subs.upgrade() {
                    s.on_down(up, &wanted.keys().cloned().collect::<Vec<_>>());
                }
                continue;
            }
            Err(_) => {
                shared.health.record_upstream(up, Signal::Failure("websocket connect timeout".into()));
                if let Some(s) = subs.upgrade() {
                    s.on_down(up, &wanted.keys().cloned().collect::<Vec<_>>());
                }
                continue;
            }
        };
        tracing::info!(upstream = %name, connection = conn, subscriptions = wanted.len(), "websocket connected");
        let (mut sink, mut stream) = ws.split();

        let mut next_req: u64 = 1;
        let mut pending: HashMap<u64, SubKey> = HashMap::new();
        let mut active: HashMap<u64, SubKey> = HashMap::new();
        let mut by_key: HashMap<SubKey, u64> = HashMap::new();
        let mut send_failed = false;

        for (key, (method, params)) in &wanted {
            let id = next_req;
            next_req += 1;
            pending.insert(id, key.clone());
            if sink.send(Message::Text(request(id, method, params.as_ref()).into())).await.is_err() {
                send_failed = true;
                break;
            }
        }

        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.tick().await;
        let mut last_seen = Instant::now();
        let reason: String = if send_failed {
            "send failed".into()
        } else {
            loop {
                tokio::select! {
                    cmd = rx.recv() => {
                        let Some(cmd) = cmd else { return };
                        match cmd {
                            Cmd::Want { key, method, params } => {
                                if wanted.contains_key(&key) { continue; }
                                let id = next_req;
                                next_req += 1;
                                pending.insert(id, key.clone());
                                let text = request(id, &method, params.as_ref());
                                wanted.insert(key, (method, params));
                                if sink.send(Message::Text(text.into())).await.is_err() { break "send failed".into(); }
                            }
                            Cmd::Unwant { key } => {
                                let Some((method, _)) = wanted.remove(&key) else { continue };
                                if let Some(sub_id) = by_key.remove(&key) {
                                    active.remove(&sub_id);
                                    let unsub = solana::subscription(&method).map_or("", |s| s.unsubscribe);
                                    let id = next_req;
                                    next_req += 1;
                                    let text = request(id, unsub, Some(&json!([sub_id])));
                                    if sink.send(Message::Text(text.into())).await.is_err() { break "send failed".into(); }
                                }
                                // A subscribe still pending is unsubscribed when its
                                // confirmation arrives (it is no longer wanted).
                            }
                        }
                    }
                    msg = stream.next() => {
                        last_seen = Instant::now();
                        let text = match msg {
                            None => break "closed".into(),
                            Some(Err(e)) => break strip_url(&e.to_string()),
                            Some(Ok(Message::Text(t))) => t.as_str().to_owned(),
                            Some(Ok(Message::Binary(b))) => String::from_utf8_lossy(&b).into_owned(),
                            Some(Ok(Message::Close(_))) => break "closed by upstream".into(),
                            Some(Ok(_)) => continue,
                        };
                        let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
                        let Some(subs) = subs.upgrade() else { return };
                        if let Some(id) = v.get("id").and_then(Value::as_u64) {
                            let Some(key) = pending.remove(&id) else { continue };
                            if let Some(err) = v.get("error") {
                                let err: RpcError = serde_json::from_value(err.clone())
                                    .unwrap_or_else(|_| RpcError::new(jsonrpc::UPSTREAM_FAILED, "upstream failed"));
                                wanted.remove(&key);
                                subs.on_confirmed(up, &key, Err(err));
                            } else if let Some(sub_id) = v.get("result").and_then(Value::as_u64) {
                                if wanted.contains_key(&key) {
                                    active.insert(sub_id, key.clone());
                                    by_key.insert(key.clone(), sub_id);
                                    subs.on_confirmed(up, &key, Ok(()));
                                } else {
                                    // Unwanted while pending: close it upstream.
                                    let unsub = solana::subscription(&key.method).map_or("", |s| s.unsubscribe);
                                    let id = next_req;
                                    next_req += 1;
                                    let text = request(id, unsub, Some(&json!([sub_id])));
                                    if sink.send(Message::Text(text.into())).await.is_err() { break "send failed".into(); }
                                }
                            }
                        } else if let Some(params) = v.get("params") {
                            let Some(sub_id) = params.get("subscription").and_then(Value::as_u64) else { continue };
                            let Some(key) = active.get(&sub_id).cloned() else { continue };
                            if let Some(result) = params.get("result") {
                                subs.on_notification(up, &key, result);
                            }
                            if key.method == "signatureSubscribe"
                                && params.pointer("/result/value").and_then(Value::as_str) != Some("receivedSignature")
                            {
                                // The node ends signature subscriptions itself.
                                active.remove(&sub_id);
                                by_key.remove(&key);
                            }
                        }
                    }
                    _ = ping.tick() => {
                        if last_seen.elapsed() > PING_INTERVAL * 3 {
                            break "ping timeout".into();
                        }
                        if sink.send(Message::Ping(Vec::new().into())).await.is_err() { break "send failed".into(); }
                    }
                }
                if wanted.is_empty() && pending.is_empty() {
                    let _ = sink.close().await;
                    break String::new();
                }
            }
        };
        if reason.is_empty() {
            // Nothing left to hold: closed on purpose.
            continue;
        }
        tracing::warn!(upstream = %name, connection = conn, "websocket dropped: {reason}");
        shared.health.record_upstream(up, Signal::Failure(format!("websocket {reason}")));
        // Signature subscriptions already answered are gone for good.
        if let Some(s) = subs.upgrade() {
            s.on_down(up, &wanted.keys().cloned().collect::<Vec<_>>());
        }
    }
}

fn apply_offline(wanted: &mut HashMap<SubKey, (String, Option<Value>)>, cmd: Cmd) {
    match cmd {
        Cmd::Want { key, method, params } => {
            wanted.insert(key, (method, params));
        }
        Cmd::Unwant { key } => {
            wanted.remove(&key);
        }
    }
}

fn request(id: u64, method: &str, params: Option<&Value>) -> String {
    match params {
        Some(p) => json!({"jsonrpc": "2.0", "id": id, "method": method, "params": p}).to_string(),
        None => json!({"jsonrpc": "2.0", "id": id, "method": method}).to_string(),
    }
}

/// Error strings from the WebSocket client may include the URL, which holds
/// secrets: drop them when they do.
fn strip_url(s: &str) -> String {
    if s.contains("://") {
        "connection error (details omitted: they contain the upstream URL)".to_owned()
    } else {
        s.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_drops_only_duplicates_and_older_state() {
        let h = |s: &str| blake3::hash(s.as_bytes());
        let mut events = Dedup::new(false);
        assert!(events.accept(Some(101), h("a")));
        assert!(!events.accept(Some(101), h("a")), "the second upstream's copy");
        assert!(events.accept(Some(100), h("b")), "a late event nobody sent yet");
        assert!(!events.accept(Some(100), h("b")));
        for i in 0..500 {
            assert!(events.accept(Some(102), h(&format!("e{i}"))));
        }
        assert!(!events.accept(Some(102), h("e0")), "a busy slot does not push ids out");
        assert!(events.accept(None, h("x")));
        assert!(!events.accept(None, h("x")));

        let mut state = Dedup::new(true);
        assert!(state.accept(Some(101), h("v5")));
        assert!(!state.accept(Some(100), h("v3")), "older state after newer");
        assert!(state.accept(Some(101), h("v6")), "a new state in the same slot");
        assert!(!state.accept(Some(101), h("v6")));
    }

    #[test]
    fn settle_rules() {
        assert!(settle(&[UpStatus::Pending, UpStatus::Down]).is_none());
        assert!(matches!(settle(&[UpStatus::Active, UpStatus::Down]), Some(Ok(()))));
        match settle(&[UpStatus::Failed(RpcError::new(-32602, "bad")), UpStatus::Down]) {
            Some(Err(Answer::Error(e))) => assert_eq!(e.code, -32602),
            _ => panic!(),
        }
        match settle(&[UpStatus::Down]) {
            Some(Err(Answer::Error(e))) => assert_eq!(e.code, -33095),
            _ => panic!(),
        }
    }

    fn client(method: &str, conn: Arc<ConnHandle>) -> ClientSub {
        ClientSub {
            conn,
            owner: 0,
            key: SubKey::new(method, None),
            method: method.to_owned(),
            params: None,
            plan: Arc::new(EffectivePolicies {
                selector: delta_engine::selector::Selector::new(vec![0], delta_engine::selector::Strategy::RoundRobin),
                cache: None,
                mode: Mode::Single,
                retry: None,
            }),
            upstreams: vec![0],
            tried: Vec::new(),
            waiter: None,
            gap: false,
            dedup: Dedup::default(),
            acked: true,
            held: Vec::new(),
            finish_on_ack: false,
            latest: Arc::new(LatestSlot::default()),
            queued_events: Arc::new(AtomicUsize::new(0)),
            last_value: None,
        }
    }

    #[test]
    fn slow_clients_keep_every_event_but_only_the_latest_state() {
        let (conn, mut rx, close_rx) = ConnHandle::new(1);
        let program = client("programSubscribe", conn.clone());
        let account = client("accountSubscribe", conn.clone());
        for i in 0..3 {
            deliver(&program, 7, &format!(r#"{{"account":{i}}}"#));
            deliver(&account, 8, &format!(r#"{{"lamports":{i}}}"#));
        }
        let (mut events, mut states) = (Vec::new(), Vec::new());
        while let Ok(out) = rx.try_recv() {
            match out {
                Outgoing::Event(_, t) => events.push(t),
                Outgoing::Latest(slot) => states.extend(slot.take()),
                Outgoing::Text(_) | Outgoing::Response(..) => {}
            }
        }
        assert_eq!(events.len(), 3, "every program notification is a different account");
        assert!(events.iter().all(|e| e.contains("programNotification")));
        assert_eq!(states.len(), 1, "an account subscription keeps only the newest state");
        assert!(states[0].contains(r#""lamports":2"#), "{}", states[0]);
        assert!(close_rx.borrow().is_none());
    }

    #[test]
    fn an_event_overflow_closes_the_client() {
        let (conn, _rx, close_rx) = ConnHandle::new(1);
        let logs = client("logsSubscribe", conn);
        for _ in 0..EVENT_BUFFER {
            deliver(&logs, 1, "{}");
        }
        assert!(close_rx.borrow().is_none(), "a full buffer is fine");
        deliver(&logs, 1, "{}");
        let reason = close_rx.borrow().clone().expect("closed");
        assert!(reason.contains("logsSubscribe") && reason.contains("overflow"), "{reason}");
    }

    #[test]
    fn websocket_errors_drop_urls() {
        assert!(!strip_url("IO error: wss://h/?api-key=secret refused").contains("secret"));
        assert_eq!(strip_url("Connection reset"), "Connection reset");
    }
}
