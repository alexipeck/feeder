use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct SchedulerStats {
    pub worker_count: AtomicUsize,
    pub ingress_accepted: AtomicU64,
    pub ingress_consumed: AtomicU64,
    pub demand_commands: AtomicU64,
    pub demand_requested: AtomicU64,
    pub demand_fulfilled: AtomicU64,
    pub demand_abandoned: AtomicU64,
    pub recovered_enqueued: AtomicU64,
    pub recovered_routed: AtomicU64,
    pub output_routed: AtomicU64,
    pub receiver_drop_recovered: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SchedulerStatsSnapshot {
    pub worker_count: usize,
    pub ingress_accepted: u64,
    pub ingress_consumed: u64,
    pub demand_commands: u64,
    pub demand_requested: u64,
    pub demand_fulfilled: u64,
    pub demand_abandoned: u64,
    pub recovered_enqueued: u64,
    pub recovered_routed: u64,
    pub output_routed: u64,
    pub receiver_drop_recovered: u64,
    pub ingress_pending: usize,
    pub demand_pending: usize,
    pub recovered_pending: usize,
    pub active_worker_items: usize,
    pub delivered_inflight: usize,
}

impl SchedulerStats {
    pub fn snapshot_pending(
        &self,
        ingress_pending: usize,
        demand_pending: usize,
        recovered_pending: usize,
        active_worker_items: usize,
        delivered_inflight: usize,
    ) -> SchedulerStatsSnapshot {
        SchedulerStatsSnapshot {
            worker_count: self.worker_count.load(Ordering::Relaxed),
            ingress_accepted: self.ingress_accepted.load(Ordering::Relaxed),
            ingress_consumed: self.ingress_consumed.load(Ordering::Relaxed),
            demand_commands: self.demand_commands.load(Ordering::Relaxed),
            demand_requested: self.demand_requested.load(Ordering::Relaxed),
            demand_fulfilled: self.demand_fulfilled.load(Ordering::Relaxed),
            demand_abandoned: self.demand_abandoned.load(Ordering::Relaxed),
            recovered_enqueued: self.recovered_enqueued.load(Ordering::Relaxed),
            recovered_routed: self.recovered_routed.load(Ordering::Relaxed),
            output_routed: self.output_routed.load(Ordering::Relaxed),
            receiver_drop_recovered: self.receiver_drop_recovered.load(Ordering::Relaxed),
            ingress_pending,
            demand_pending,
            recovered_pending,
            active_worker_items,
            delivered_inflight,
        }
    }

    pub fn reset(&self) {
        self.ingress_accepted.store(0, Ordering::Relaxed);
        self.ingress_consumed.store(0, Ordering::Relaxed);
        self.demand_commands.store(0, Ordering::Relaxed);
        self.demand_requested.store(0, Ordering::Relaxed);
        self.demand_fulfilled.store(0, Ordering::Relaxed);
        self.demand_abandoned.store(0, Ordering::Relaxed);
        self.recovered_enqueued.store(0, Ordering::Relaxed);
        self.recovered_routed.store(0, Ordering::Relaxed);
        self.output_routed.store(0, Ordering::Relaxed);
        self.receiver_drop_recovered.store(0, Ordering::Relaxed);
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
        eprintln!("    worker_count:           {}", s.worker_count);
        eprintln!("    ingress_accepted:       {}", s.ingress_accepted);
        eprintln!("    ingress_consumed:       {}", s.ingress_consumed);
        eprintln!("    demand_commands:        {}", s.demand_commands);
        eprintln!("    demand_requested:       {}", s.demand_requested);
        eprintln!("    demand_fulfilled:       {}", s.demand_fulfilled);
        eprintln!("    demand_abandoned:       {}", s.demand_abandoned);
        eprintln!("    recovered_enqueued:     {}", s.recovered_enqueued);
        eprintln!("    recovered_routed:       {}", s.recovered_routed);
        eprintln!("    output_routed:          {}", s.output_routed);
        eprintln!("    receiver_drop_recovered: {}", s.receiver_drop_recovered);
        eprintln!("    ingress_pending:        {}", s.ingress_pending);
        eprintln!("    demand_pending:         {}", s.demand_pending);
        eprintln!("    recovered_pending:      {}", s.recovered_pending);
        eprintln!("    active_worker_items:    {}", s.active_worker_items);
        eprintln!("    delivered_inflight:     {}", s.delivered_inflight);
        eprintln!("  consumers (aggregated):");
        eprintln!("    get_calls:              {}", c.get_calls);
        eprintln!("    items_consumed:         {}", c.items_consumed);
        eprintln!("    refill_messages:        {}", c.refill_messages);
        if c.items_consumed > 0 {
            let routed = s.output_routed;
            eprintln!("  ratios:");
            eprintln!(
                "    refill_messages / items_consumed: {:.4}",
                c.refill_messages as f64 / c.items_consumed as f64
            );
            if s.demand_commands > 0 {
                eprintln!(
                    "    demand_requested / command: {:.4}",
                    s.demand_requested as f64 / s.demand_commands as f64
                );
            }
            eprintln!(
                "    output_routed / items_consumed:   {:.4}",
                routed as f64 / c.items_consumed as f64
            );
        }
        eprintln!();
    }
}
