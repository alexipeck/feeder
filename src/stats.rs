use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct SchedulerStats {
    pub ingress_routed: AtomicU64,
    pub ingress_closed: AtomicU64,
    pub recovered_fulfilled: AtomicU64,
    pub demand_queued: AtomicU64,
    pub head_advanced: AtomicU64,
    pub head_stale: AtomicU64,
    pub select_ingress_wins: AtomicU64,
    pub select_control_wins: AtomicU64,
    pub select_demand_wins: AtomicU64,
    pub select_idle_timeouts: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SchedulerStatsSnapshot {
    pub ingress_routed: u64,
    pub ingress_closed: u64,
    pub recovered_fulfilled: u64,
    pub demand_queued: u64,
    pub head_advanced: u64,
    pub head_stale: u64,
    pub select_ingress_wins: u64,
    pub select_control_wins: u64,
    pub select_demand_wins: u64,
    pub select_idle_timeouts: u64,
}

impl SchedulerStats {
    pub fn snapshot(&self) -> SchedulerStatsSnapshot {
        SchedulerStatsSnapshot {
            ingress_routed: self.ingress_routed.load(Ordering::Relaxed),
            ingress_closed: self.ingress_closed.load(Ordering::Relaxed),
            recovered_fulfilled: self.recovered_fulfilled.load(Ordering::Relaxed),
            demand_queued: self.demand_queued.load(Ordering::Relaxed),
            head_advanced: self.head_advanced.load(Ordering::Relaxed),
            head_stale: self.head_stale.load(Ordering::Relaxed),
            select_ingress_wins: self.select_ingress_wins.load(Ordering::Relaxed),
            select_control_wins: self.select_control_wins.load(Ordering::Relaxed),
            select_demand_wins: self.select_demand_wins.load(Ordering::Relaxed),
            select_idle_timeouts: self.select_idle_timeouts.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.ingress_routed.store(0, Ordering::Relaxed);
        self.ingress_closed.store(0, Ordering::Relaxed);
        self.recovered_fulfilled.store(0, Ordering::Relaxed);
        self.demand_queued.store(0, Ordering::Relaxed);
        self.head_advanced.store(0, Ordering::Relaxed);
        self.head_stale.store(0, Ordering::Relaxed);
        self.select_ingress_wins.store(0, Ordering::Relaxed);
        self.select_control_wins.store(0, Ordering::Relaxed);
        self.select_demand_wins.store(0, Ordering::Relaxed);
        self.select_idle_timeouts.store(0, Ordering::Relaxed);
    }

    pub fn inc_ingress_routed(&self, n: u64) {
        self.ingress_routed.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_recovered_fulfilled(&self, n: u64) {
        self.recovered_fulfilled.fetch_add(n, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
pub struct ConsumerStats {
    pub get_calls: AtomicU64,
    pub items_consumed: AtomicU64,
    pub refill_messages: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConsumerStatsSnapshot {
    pub get_calls: u64,
    pub items_consumed: u64,
    pub refill_messages: u64,
}

impl ConsumerStats {
    pub fn snapshot(&self) -> ConsumerStatsSnapshot {
        ConsumerStatsSnapshot {
            get_calls: self.get_calls.load(Ordering::Relaxed),
            items_consumed: self.items_consumed.load(Ordering::Relaxed),
            refill_messages: self.refill_messages.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.get_calls.store(0, Ordering::Relaxed);
        self.items_consumed.store(0, Ordering::Relaxed);
        self.refill_messages.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FeederStatsSnapshot {
    pub scheduler: SchedulerStatsSnapshot,
    pub consumers: ConsumerStatsSnapshot,
}

impl FeederStatsSnapshot {
    pub fn print_stderr(&self, label: &str) {
        let s = &self.scheduler;
        let c = &self.consumers;
        eprintln!();
        eprintln!("feeder perf-stats: {label}");
        eprintln!("  scheduler:");
        eprintln!("    ingress_routed:       {}", s.ingress_routed);
        eprintln!("    recovered_fulfilled:  {}", s.recovered_fulfilled);
        eprintln!("    demand_queued:          {}", s.demand_queued);
        eprintln!("    head_advanced:          {}", s.head_advanced);
        eprintln!("    head_stale:             {}", s.head_stale);
        eprintln!("    select_ingress_wins:    {}", s.select_ingress_wins);
        eprintln!("    select_control_wins:    {}", s.select_control_wins);
        eprintln!("    select_demand_wins:     {}", s.select_demand_wins);
        eprintln!("    select_idle_timeouts:   {}", s.select_idle_timeouts);
        eprintln!("    ingress_closed:         {}", s.ingress_closed);
        eprintln!("  consumers (aggregated):");
        eprintln!("    get_calls:              {}", c.get_calls);
        eprintln!("    items_consumed:         {}", c.items_consumed);
        eprintln!("    refill_messages:        {}", c.refill_messages);
        if c.items_consumed > 0 {
            let routed = s.ingress_routed + s.recovered_fulfilled;
            eprintln!("  ratios:");
            eprintln!(
                "    refill_messages / items_consumed: {:.4}",
                c.refill_messages as f64 / c.items_consumed as f64
            );
            eprintln!(
                "    ingress_routed / routed_total:    {:.4}",
                s.ingress_routed as f64 / routed as f64
            );
            eprintln!(
                "    head_advanced / ingress_routed:   {:.4}",
                s.head_advanced as f64 / s.ingress_routed.max(1) as f64
            );
            eprintln!(
                "    head_stale / ingress_routed:      {:.4}",
                s.head_stale as f64 / s.ingress_routed.max(1) as f64
            );
        }
        eprintln!();
    }
}
