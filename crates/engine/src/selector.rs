//! Load balancing: the selector every policy asks for upstreams. The
//! interface is "pick N, excluding these", not "pick one".

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use rand::seq::SliceRandom;
use rand::Rng;

use crate::health::Availability;
use crate::shared::Shared;
use crate::upstream::UpstreamId;

#[derive(Debug)]
pub enum Strategy {
    RoundRobin,
    Random,
    /// One weight per candidate, in candidate order (each at least 1).
    Weighted(Vec<u32>),
    /// Ranks of candidate indices, best first. Upstreams in one rank share it
    /// and requests rotate between them.
    Priority(Vec<Vec<usize>>),
}

#[derive(Debug)]
pub struct Selector {
    candidates: Vec<UpstreamId>,
    strategy: Strategy,
    counter: AtomicUsize,
}

/// Selection tiers, best first. Overloaded and unhealthy upstreams are used
/// only when nothing better remains.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Tier {
    Selectable,
    AtLimit,
    Overloaded,
    Unhealthy,
}

impl Selector {
    pub fn new(candidates: Vec<UpstreamId>, strategy: Strategy) -> Self {
        if let Strategy::Weighted(w) = &strategy {
            assert_eq!(w.len(), candidates.len());
        }
        Self { candidates, strategy, counter: AtomicUsize::new(0) }
    }

    pub fn candidates(&self) -> &[UpstreamId] {
        &self.candidates
    }

    /// Picks up to `n` distinct upstreams, none of them in `exclude`.
    pub fn pick(&self, shared: &Shared, method: &str, n: usize, exclude: &[UpstreamId]) -> Vec<UpstreamId> {
        let now = Instant::now();
        let mut tiers: [Vec<usize>; 4] = Default::default();
        for (idx, &id) in self.candidates.iter().enumerate() {
            if exclude.contains(&id) {
                continue;
            }
            let avail = shared.health.availability(id, method, now);
            let tier = match avail {
                Availability::Healthy | Availability::Probe if shared.at_limit(id) => Tier::AtLimit,
                Availability::Healthy | Availability::Probe => Tier::Selectable,
                Availability::Overloaded => Tier::Overloaded,
                Availability::Unhealthy => Tier::Unhealthy,
            };
            tiers[tier as usize].push(idx);
        }

        let mut chosen: Vec<usize> = self.choose(&tiers[Tier::Selectable as usize], n);
        if chosen.len() < n {
            // Upstreams at their in-flight limit take the overflow.
            let rest = n - chosen.len();
            chosen.extend(self.choose(&tiers[Tier::AtLimit as usize], rest));
        }
        if chosen.is_empty() {
            // Nothing healthy remains: an overloaded upstream before an
            // unhealthy one, rather than failing outright.
            for tier in [Tier::Overloaded, Tier::Unhealthy] {
                chosen = self.choose(&tiers[tier as usize], n);
                if !chosen.is_empty() {
                    break;
                }
            }
        }

        let picked: Vec<UpstreamId> = chosen.into_iter().map(|i| self.candidates[i]).collect();
        for &id in &picked {
            shared.health.begin_probe(id, method, now);
        }
        picked
    }

    /// Chooses up to `n` candidate indices from `pool` with this strategy.
    fn choose(&self, pool: &[usize], n: usize) -> Vec<usize> {
        if pool.is_empty() || n == 0 {
            return Vec::new();
        }
        let n = n.min(pool.len());
        let mut rng = rand::thread_rng();
        match &self.strategy {
            Strategy::RoundRobin => {
                let start = self.counter.fetch_add(1, Ordering::Relaxed);
                (0..n).map(|k| pool[(start + k) % pool.len()]).collect()
            }
            Strategy::Random => pool.choose_multiple(&mut rng, n).copied().collect(),
            Strategy::Weighted(weights) => {
                let mut remaining: Vec<usize> = pool.to_vec();
                let mut out = Vec::with_capacity(n);
                while out.len() < n {
                    let total: u64 = remaining.iter().map(|&i| weights[i] as u64).sum();
                    let mut r = rng.gen_range(0..total);
                    let pos = remaining
                        .iter()
                        .position(|&i| {
                            let w = weights[i] as u64;
                            if r < w {
                                true
                            } else {
                                r -= w;
                                false
                            }
                        })
                        .expect("r < total");
                    out.push(remaining.swap_remove(pos));
                }
                out
            }
            Strategy::Priority(ranks) => {
                let start = self.counter.fetch_add(1, Ordering::Relaxed);
                let mut out = Vec::with_capacity(n);
                for rank in ranks {
                    let members: Vec<usize> = rank.iter().copied().filter(|i| pool.contains(i)).collect();
                    for k in 0..members.len() {
                        if out.len() == n {
                            return out;
                        }
                        out.push(members[(start + k) % members.len()]);
                    }
                }
                out
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::NoCache;
    use crate::health::Signal;
    use crate::limits::Limits;
    use crate::upstream::{HttpTransport, UpstreamInfo};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn shared(n: usize) -> Arc<Shared> {
        let ups = (0..n)
            .map(|id| UpstreamInfo { id, name: format!("u{id}"), ..UpstreamInfo::default() })
            .collect();
        Shared::new(ups, Arc::new(NoCache), Arc::new(HttpTransport::new()), Limits::default())
    }

    #[test]
    fn priority_uses_first_healthy() {
        let s = shared(3);
        let sel = Selector::new(vec![0, 1, 2], Strategy::Priority(vec![vec![0], vec![1], vec![2]]));
        assert_eq!(sel.pick(&s, "m", 1, &[]), vec![0]);
        assert_eq!(sel.pick(&s, "m", 2, &[]), vec![0, 1]);
        s.health.record(0, "m", Signal::Overloaded, Signal::Neutral);
        assert_eq!(sel.pick(&s, "m", 1, &[]), vec![1]);
        assert_eq!(sel.pick(&s, "m", 1, &[1]), vec![2]);
    }

    #[test]
    fn priority_rotates_within_rank() {
        let s = shared(3);
        let sel = Selector::new(vec![0, 1, 2], Strategy::Priority(vec![vec![0, 1], vec![2]]));
        let a = sel.pick(&s, "m", 1, &[]);
        let b = sel.pick(&s, "m", 1, &[]);
        assert_ne!(a, b);
        assert!(a[0] < 2 && b[0] < 2);
    }

    #[test]
    fn falls_back_to_overloaded_before_unhealthy() {
        let s = shared(2);
        let sel = Selector::new(vec![0, 1], Strategy::RoundRobin);
        for _ in 0..5 {
            s.health.record(0, "m", Signal::Failure("x".into()), Signal::Neutral);
        }
        s.health.record(1, "m", Signal::Overloaded, Signal::Neutral);
        assert_eq!(sel.pick(&s, "m", 1, &[]), vec![1]);
    }

    #[test]
    fn weighted_distribution() {
        let s = shared(3);
        let sel = Selector::new(vec![0, 1, 2], Strategy::Weighted(vec![5, 2, 1]));
        let mut counts: HashMap<UpstreamId, usize> = HashMap::new();
        for _ in 0..8000 {
            *counts.entry(sel.pick(&s, "m", 1, &[])[0]).or_default() += 1;
        }
        let share0 = counts[&0] as f64 / 8000.0;
        assert!((share0 - 0.625).abs() < 0.05, "{share0}");
        // Several picks never repeat.
        let picks = sel.pick(&s, "m", 3, &[]);
        let mut sorted = picks.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 3);
    }
}
