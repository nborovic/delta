//! Upstream health at two levels: per upstream and per (upstream, method).
//! State is shared by every route.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rand::Rng;

use crate::limits::HealthLimits;
use crate::upstream::UpstreamId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Healthy,
    Overloaded,
    Unhealthy,
}

/// How a selector may treat an upstream right now, best first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Availability {
    Healthy,
    /// In a bad state whose backoff has run out: one real request may go
    /// through as a test.
    Probe,
    Overloaded,
    Unhealthy,
}

impl Availability {
    pub fn selectable(self) -> bool {
        matches!(self, Availability::Healthy | Availability::Probe)
    }
}

/// What one call tells a health record.
#[derive(Clone, Debug, PartialEq)]
pub enum Signal {
    Success,
    /// Counts toward the Unhealthy thresholds.
    Failure(String),
    /// Trips Overloaded at once. A 429's `Retry-After` is not used: providers
    /// send it inconsistently, so the backoff is the same either way.
    Overloaded,
    /// Not counted (the request's fault, or cut off by the request deadline).
    Neutral,
}

#[derive(Debug)]
struct Record {
    state: State,
    strikes: u32,
    /// Time of the last trip (a failure that set or kept a bad state). Strikes
    /// reset when this is older than DECAY.
    last_failure: Option<Instant>,
    until: Instant,
    probe_started: Option<Instant>,
    window: VecDeque<(Instant, bool)>,
    consecutive: u32,
}

impl Record {
    fn new(now: Instant) -> Self {
        Self {
            state: State::Healthy,
            strikes: 0,
            last_failure: None,
            until: now,
            probe_started: None,
            window: VecDeque::new(),
            consecutive: 0,
        }
    }

    fn availability(&self, now: Instant, limits: &HealthLimits) -> Availability {
        let bad = match self.state {
            State::Healthy => return Availability::Healthy,
            State::Overloaded => Availability::Overloaded,
            State::Unhealthy => Availability::Unhealthy,
        };
        if now < self.until {
            return bad;
        }
        match self.probe_started {
            Some(t) if now.duration_since(t) < limits.probe_timeout => bad,
            _ => Availability::Probe,
        }
    }

    /// Applies a signal; returns a log line when the state changed.
    fn apply(&mut self, signal: &Signal, now: Instant, limits: &HealthLimits) -> Option<String> {
        let probing = self.state != State::Healthy && now >= self.until;
        match signal {
            Signal::Neutral => {
                if probing {
                    self.probe_started = None;
                }
                None
            }
            Signal::Success => {
                if self.state != State::Healthy {
                    if !probing {
                        // A call started before the trip; it says nothing
                        // about the node now.
                        return None;
                    }
                    self.state = State::Healthy;
                    self.probe_started = None;
                    self.window.clear();
                    self.consecutive = 0;
                    return Some("healthy".to_owned());
                }
                self.push(now, true, limits);
                self.consecutive = 0;
                None
            }
            Signal::Overloaded => {
                if self.state != State::Healthy && !probing {
                    return None;
                }
                Some(self.trip(State::Overloaded, limits.overloaded_base, limits.overloaded_cap, now, limits, "429"))
            }
            Signal::Failure(reason) => {
                if self.state != State::Healthy {
                    if !probing {
                        return None;
                    }
                    return Some(self.trip(
                        State::Unhealthy,
                        limits.unhealthy_base,
                        limits.unhealthy_cap,
                        now,
                        limits,
                        reason,
                    ));
                }
                self.push(now, false, limits);
                self.consecutive += 1;
                let failures = self.window.iter().filter(|(_, ok)| !ok).count();
                let total = self.window.len();
                let ratio_trip = total >= limits.min_requests
                    && failures as f64 >= limits.failure_ratio * total as f64;
                if self.consecutive >= limits.consecutive_failures || ratio_trip {
                    return Some(self.trip(
                        State::Unhealthy,
                        limits.unhealthy_base,
                        limits.unhealthy_cap,
                        now,
                        limits,
                        reason,
                    ));
                }
                None
            }
        }
    }

    fn push(&mut self, now: Instant, ok: bool, limits: &HealthLimits) {
        self.window.push_back((now, ok));
        while let Some(&(t, _)) = self.window.front() {
            if now.duration_since(t) > limits.window {
                self.window.pop_front();
            } else {
                break;
            }
        }
    }

    fn trip(
        &mut self,
        state: State,
        base: Duration,
        cap: Duration,
        now: Instant,
        limits: &HealthLimits,
        reason: &str,
    ) -> String {
        let decayed = self
            .last_failure
            .is_none_or(|t| now.duration_since(t) > limits.decay);
        if decayed {
            self.strikes = 0;
        }
        self.strikes += 1;
        self.last_failure = Some(now);
        let exp = self.strikes.saturating_sub(1).min(20);
        let backoff = base.saturating_mul(1u32 << exp).min(cap);
        let factor = 1.0 + rand::thread_rng().gen_range(-limits.jitter..=limits.jitter);
        let backoff = backoff.mul_f64(factor);
        self.state = state;
        self.until = now + backoff;
        self.probe_started = None;
        self.window.clear();
        self.consecutive = 0;
        let label = match state {
            State::Overloaded => "overloaded",
            State::Unhealthy => "unhealthy",
            State::Healthy => "healthy",
        };
        format!("{label} ({reason}) for {:.1} s", backoff.as_secs_f64())
    }
}

pub struct Health {
    limits: HealthLimits,
    names: Vec<String>,
    upstreams: Vec<Mutex<Record>>,
    methods: Mutex<HashMap<(UpstreamId, String), Record>>,
}

impl Health {
    pub fn new(names: Vec<String>, limits: HealthLimits) -> Self {
        let now = Instant::now();
        Self {
            upstreams: names.iter().map(|_| Mutex::new(Record::new(now))).collect(),
            names,
            limits,
            methods: Mutex::new(HashMap::new()),
        }
    }

    /// The worse of the upstream's and the (upstream, method) availability.
    pub fn availability(&self, id: UpstreamId, method: &str, now: Instant) -> Availability {
        let up = self.upstreams[id].lock().unwrap().availability(now, &self.limits);
        let m = self
            .methods
            .lock()
            .unwrap()
            .get(&(id, method.to_owned()))
            .map_or(Availability::Healthy, |r| r.availability(now, &self.limits));
        up.max(m)
    }

    /// Marks a probe as in flight on whichever record is being tested, so
    /// that other requests keep avoiding it.
    pub fn begin_probe(&self, id: UpstreamId, method: &str, now: Instant) {
        let mut up = self.upstreams[id].lock().unwrap();
        if up.availability(now, &self.limits) == Availability::Probe {
            up.probe_started = Some(now);
        }
        drop(up);
        if let Some(r) = self.methods.lock().unwrap().get_mut(&(id, method.to_owned())) {
            if r.availability(now, &self.limits) == Availability::Probe {
                r.probe_started = Some(now);
            }
        }
    }

    /// Records one call's outcome at the upstream level and the method level.
    pub fn record(&self, id: UpstreamId, method: &str, upstream: Signal, per_method: Signal) {
        let now = Instant::now();
        if let Some(change) = self.upstreams[id].lock().unwrap().apply(&upstream, now, &self.limits) {
            log_change(&self.names[id], None, &change);
        }
        let mut methods = self.methods.lock().unwrap();
        let key = (id, method.to_owned());
        let record = match (&per_method, methods.get_mut(&key)) {
            (_, Some(r)) => r,
            // Only create a per-method record when something can go wrong.
            (Signal::Failure(_) | Signal::Overloaded, None) => {
                methods.entry(key).or_insert_with(|| Record::new(now))
            }
            _ => return,
        };
        if let Some(change) = record.apply(&per_method, now, &self.limits) {
            log_change(&self.names[id], Some(method), &change);
        }
    }

    /// Shortcut for a failure of the whole upstream (e.g. a dropped WebSocket).
    pub fn record_upstream(&self, id: UpstreamId, signal: Signal) {
        let now = Instant::now();
        if let Some(change) = self.upstreams[id].lock().unwrap().apply(&signal, now, &self.limits) {
            log_change(&self.names[id], None, &change);
        }
    }

    /// When the upstream (ignoring per-method records) may be used again.
    pub fn upstream_ready_at(&self, id: UpstreamId) -> Instant {
        let r = self.upstreams[id].lock().unwrap();
        match r.state {
            State::Healthy => Instant::now(),
            _ => r.until,
        }
    }
}

fn log_change(upstream: &str, method: Option<&str>, change: &str) {
    match method {
        Some(m) => tracing::warn!(target: "delta::health", "{upstream} {m} {change}"),
        None => tracing::warn!(target: "delta::health", "{upstream} {change}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> HealthLimits {
        HealthLimits { jitter: 0.0, ..HealthLimits::default() }
    }

    #[test]
    fn five_consecutive_failures_trip() {
        let l = limits();
        let now = Instant::now();
        let mut r = Record::new(now);
        for i in 0..4 {
            assert!(r.apply(&Signal::Failure("x".into()), now, &l).is_none(), "{i}");
        }
        let change = r.apply(&Signal::Failure("x".into()), now, &l).unwrap();
        assert!(change.starts_with("unhealthy"));
        assert_eq!(r.availability(now, &l), Availability::Unhealthy);
        assert_eq!(r.until - now, Duration::from_secs(5));
    }

    #[test]
    fn success_resets_consecutive() {
        let l = limits();
        let now = Instant::now();
        let mut r = Record::new(now);
        for _ in 0..4 {
            r.apply(&Signal::Failure("x".into()), now, &l);
        }
        r.apply(&Signal::Success, now, &l);
        for _ in 0..4 {
            assert!(r.apply(&Signal::Failure("x".into()), now, &l).is_none());
        }
    }

    #[test]
    fn ratio_trips_flaky_node() {
        let l = limits();
        let now = Instant::now();
        let mut r = Record::new(now);
        let mut tripped = false;
        for i in 0..20 {
            let s = if i % 2 == 0 { Signal::Success } else { Signal::Failure("x".into()) };
            tripped |= r.apply(&s, now, &l).is_some();
        }
        assert!(tripped);
    }

    #[test]
    fn overloaded_trips_at_once_and_backs_off() {
        let l = limits();
        let t0 = Instant::now();
        let mut r = Record::new(t0);
        r.apply(&Signal::Overloaded, t0, &l);
        assert_eq!(r.until - t0, Duration::from_secs(1));
        // After the backoff, a probe is allowed; its failure doubles the wait.
        let t1 = r.until;
        assert_eq!(r.availability(t1, &l), Availability::Probe);
        r.apply(&Signal::Overloaded, t1, &l);
        assert_eq!(r.until - t1, Duration::from_secs(2));
        // Probe success returns it to Healthy but keeps strikes.
        let t2 = r.until;
        r.apply(&Signal::Success, t2, &l);
        assert_eq!(r.state, State::Healthy);
        r.apply(&Signal::Overloaded, t2, &l);
        assert_eq!(r.until - t2, Duration::from_secs(4));
        // DECAY without failures resets strikes.
        let t3 = r.until + Duration::from_secs(61);
        r.apply(&Signal::Success, t3, &l);
        r.apply(&Signal::Overloaded, t3, &l);
        assert_eq!(r.until - t3, Duration::from_secs(1));
    }

    #[test]
    fn backoff_is_capped() {
        let l = limits();
        let mut t = Instant::now();
        let mut r = Record::new(t);
        for _ in 0..10 {
            r.apply(&Signal::Overloaded, t, &l);
            t = r.until;
        }
        let t0 = t;
        r.apply(&Signal::Overloaded, t0, &l);
        assert_eq!(r.until - t0, Duration::from_secs(30));
    }

    #[test]
    fn probe_is_exclusive() {
        let l = limits();
        let h = Health::new(vec!["a".into()], l.clone());
        h.record(0, "m", Signal::Overloaded, Signal::Neutral);
        let later = Instant::now() + Duration::from_secs(2);
        assert_eq!(h.availability(0, "m", later), Availability::Probe);
        h.begin_probe(0, "m", later);
        assert_eq!(h.availability(0, "m", later), Availability::Overloaded);
    }

    #[test]
    fn method_level_is_separate() {
        let h = Health::new(vec!["a".into()], limits());
        for _ in 0..5 {
            h.record(0, "getBlock", Signal::Neutral, Signal::Failure("HTTP 403".into()));
        }
        let now = Instant::now();
        assert_eq!(h.availability(0, "getBlock", now), Availability::Unhealthy);
        assert_eq!(h.availability(0, "getSlot", now), Availability::Healthy);
    }

    #[test]
    fn strikes_decay_a_minute_after_the_last_trip() {
        let l = limits();
        let s = Duration::from_secs;
        let mut r = Record::new(Instant::now());
        let trip = |r: &mut Record, t: Instant| {
            for _ in 0..5 {
                r.apply(&Signal::Failure("x".into()), t, &l);
            }
        };
        let recover = |r: &mut Record| {
            let t = r.until;
            r.apply(&Signal::Success, t, &l);
            assert_eq!(r.availability(t, &l), Availability::Healthy);
        };
        let t0 = Instant::now();
        trip(&mut r, t0);
        assert_eq!(r.until - t0, s(5));
        recover(&mut r);
        let t1 = t0 + s(15);
        trip(&mut r, t1);
        assert_eq!(r.until - t1, s(10), "a second trip within a minute doubles the backoff");
        recover(&mut r);
        // 65 s after the first trip but 50 s after the last: strikes remain.
        let t2 = t0 + s(65);
        trip(&mut r, t2);
        assert_eq!(r.until - t2, s(20), "decay counts from the last trip, not the first");
        recover(&mut r);
        let t3 = t2 + s(61);
        trip(&mut r, t3);
        assert_eq!(r.until - t3, s(5), "a minute without a trip resets the strikes");
    }
}
