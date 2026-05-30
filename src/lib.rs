use crossbeam_channel::{Receiver, RecvError, Sender, TryRecvError};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use thiserror::Error;

#[cfg(feature = "perf-stats")]
mod stats;

#[cfg(feature = "perf-stats")]
pub use stats::{ConsumerStatsSnapshot, FeederStatsSnapshot, SchedulerStatsSnapshot};

const RUNNING: u8 = 0;
const SHUTTING_DOWN: u8 = 1;
const CANCELLED: u8 = 2;
const NO_MORE_WORK: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeederId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReceiverId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoStatus;

#[derive(Debug, Clone)]
pub struct ChannelStatus {
    tx: Sender<StatusEvent>,
}

impl ChannelStatus {
    pub fn new(tx: Sender<StatusEvent>) -> Self {
        Self { tx }
    }
}

#[derive(Debug, Clone)]
pub enum StatusEvent {
    ReceiverRegistered {
        receiver_id: ReceiverId,
    },
    ReceiverDropped {
        receiver_id: ReceiverId,
        recovered: usize,
    },
    GracefulShutdownStarted,
    Cancelled,
    NoMoreWork,
    SchedulerExited,
    Lagging {
        receiver_id: ReceiverId,
        requested: usize,
        received: usize,
    },
    Recovered {
        receiver_id: ReceiverId,
    },
}

pub trait StatusSink: Clone + Send + Sync + 'static {
    fn receiver_registered(&self, receiver_id: ReceiverId);
    fn receiver_dropped(&self, receiver_id: ReceiverId, recovered: usize);
    fn graceful_shutdown_started(&self);
    fn cancelled(&self);
    fn no_more_work(&self);
    fn scheduler_exited(&self);
    fn lagging(&self, receiver_id: ReceiverId, requested: usize, received: usize);
    fn recovered(&self, receiver_id: ReceiverId);
}

impl StatusSink for NoStatus {
    fn receiver_registered(&self, _: ReceiverId) {}
    fn receiver_dropped(&self, _: ReceiverId, _: usize) {}
    fn graceful_shutdown_started(&self) {}
    fn cancelled(&self) {}
    fn no_more_work(&self) {}
    fn scheduler_exited(&self) {}
    fn lagging(&self, _: ReceiverId, _: usize, _: usize) {}
    fn recovered(&self, _: ReceiverId) {}
}

impl StatusSink for ChannelStatus {
    fn receiver_registered(&self, receiver_id: ReceiverId) {
        let _ = self
            .tx
            .send(StatusEvent::ReceiverRegistered { receiver_id });
    }
    fn receiver_dropped(&self, receiver_id: ReceiverId, recovered: usize) {
        let _ = self.tx.send(StatusEvent::ReceiverDropped {
            receiver_id,
            recovered,
        });
    }
    fn graceful_shutdown_started(&self) {
        let _ = self.tx.send(StatusEvent::GracefulShutdownStarted);
    }
    fn cancelled(&self) {
        let _ = self.tx.send(StatusEvent::Cancelled);
    }
    fn no_more_work(&self) {
        let _ = self.tx.send(StatusEvent::NoMoreWork);
    }
    fn scheduler_exited(&self) {
        let _ = self.tx.send(StatusEvent::SchedulerExited);
    }
    fn lagging(&self, receiver_id: ReceiverId, requested: usize, received: usize) {
        let _ = self.tx.send(StatusEvent::Lagging {
            receiver_id,
            requested,
            received,
        });
    }
    fn recovered(&self, receiver_id: ReceiverId) {
        let _ = self.tx.send(StatusEvent::Recovered { receiver_id });
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AccessError {
    #[error("ingress sealed")]
    IngressSealed,
    #[error("cancelled")]
    Cancelled,
    #[error("no more work")]
    NoMoreWork,
    #[error("scheduler stopped")]
    SchedulerStopped,
    #[error("low water mark must be less than high water mark")]
    InvalidWaterMarks,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError<T> {
    IngressSealed(T),
    Cancelled(T),
    NoMoreWork(T),
    SchedulerStopped(T),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendBatchError<T> {
    IngressSealed(Vec<T>),
    Cancelled(Vec<T>),
    NoMoreWork(Vec<T>),
    SchedulerStopped(Vec<T>),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum GetError {
    #[error("cancelled")]
    Cancelled,
    #[error("no more work")]
    NoMoreWork,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryGet<T> {
    Items(Vec<T>),
    Empty,
    InShutdown,
    NoMoreWork,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryGetOne<T> {
    Item(T),
    Empty,
    InShutdown,
    NoMoreWork,
    Cancelled,
}

pub struct FeederBuilder<T> {
    scheduler_threads: NonZeroUsize,
    _marker: PhantomData<T>,
}

impl<T: Send + 'static> FeederBuilder<T> {
    pub fn scheduler_threads(mut self, n: NonZeroUsize) -> Self {
        self.scheduler_threads = n;
        self
    }

    pub fn build(self) -> Feeder<T, NoStatus> {
        build_feeder(NoStatus, self.scheduler_threads)
    }

    pub fn build_with_status(self) -> (Feeder<T, ChannelStatus>, Receiver<StatusEvent>) {
        let (status_tx, status_rx) = crossbeam_channel::unbounded();
        let feeder = build_feeder(ChannelStatus::new(status_tx), self.scheduler_threads);
        (feeder, status_rx)
    }
}

impl<T: Send + 'static> Default for FeederBuilder<T> {
    fn default() -> Self {
        Self {
            scheduler_threads: NonZeroUsize::new(1).unwrap(),
            _marker: PhantomData,
        }
    }
}

struct Runtime<T: Send + 'static, S: StatusSink> {
    shared: Arc<Shared<T, S>>,
    workers: Vec<JoinHandle<()>>,
}

pub struct Feeder<T: Send + 'static, S: StatusSink = NoStatus> {
    id: FeederId,
    #[cfg(test)]
    pub(crate) runtime: Arc<Runtime<T, S>>,
    #[cfg(not(test))]
    runtime: Arc<Runtime<T, S>>,
}

pub struct FeederTx<T: Send + 'static, S: StatusSink = NoStatus> {
    runtime: Arc<Runtime<T, S>>,
}

pub struct FeederRx<T: Send + 'static, S: StatusSink = NoStatus> {
    receiver_id: ReceiverId,
    data_rx: Mutex<Option<Receiver<T>>>,
    #[cfg(test)]
    pub(crate) handle: Weak<ReceiverHandle<T>>,
    #[cfg(not(test))]
    handle: Weak<ReceiverHandle<T>>,
    low_water: usize,
    high_water: usize,
    shared: Arc<Shared<T, S>>,
    lagging: AtomicBool,
    dropped: AtomicBool,
    #[cfg(feature = "perf-stats")]
    consumer_stats: Arc<stats::ConsumerStats>,
}

struct ReceiverHandle<T> {
    _receiver_id: ReceiverId,
    data_tx: Sender<T>,
    closing: AtomicBool,
    active_sends: AtomicUsize,
    outstanding: AtomicUsize,
}

struct Shared<T: Send + 'static, S: StatusSink> {
    next_id: AtomicUsize,
    lifecycle: AtomicU8,
    accepting_ingress: AtomicBool,
    ingress_tx: Sender<T>,
    demand_tx: Sender<DemandCommand<T>>,
    recovered_tx: Sender<T>,
    registry: Mutex<HashMap<ReceiverId, Arc<ReceiverHandle<T>>>>,
    terminal_tx: Mutex<Option<Sender<()>>>,
    graceful_wake_tx: Sender<()>,
    progress_wake_tx: Sender<()>,
    ingress_pending: AtomicUsize,
    demand_pending: AtomicUsize,
    recovered_pending: AtomicUsize,
    active_worker_items: AtomicUsize,
    delivered_inflight: AtomicUsize,
    active_producer_sends: AtomicUsize,
    active_workers: AtomicUsize,
    cancelled_emitted: AtomicBool,
    no_more_work_emitted: AtomicBool,
    status: S,
    #[cfg(feature = "perf-stats")]
    scheduler_stats: Arc<stats::SchedulerStats>,
    #[cfg(feature = "perf-stats")]
    consumer_stats: Mutex<Vec<Arc<stats::ConsumerStats>>>,
}

enum DemandCommand<T> {
    Request {
        receiver: Arc<ReceiverHandle<T>>,
        count: usize,
    },
}

fn build_feeder<T: Send + 'static, S: StatusSink>(
    status: S,
    scheduler_threads: NonZeroUsize,
) -> Feeder<T, S> {
    let (ingress_tx, ingress_rx) = crossbeam_channel::unbounded();
    let (demand_tx, demand_rx) = crossbeam_channel::unbounded();
    let (recovered_tx, recovered_rx) = crossbeam_channel::unbounded();
    let (terminal_tx, terminal_rx) = crossbeam_channel::unbounded();
    let (graceful_wake_tx, graceful_wake_rx) = crossbeam_channel::unbounded();
    let (progress_wake_tx, progress_wake_rx) = crossbeam_channel::unbounded();

    #[cfg(feature = "perf-stats")]
    let scheduler_stats = Arc::new(stats::SchedulerStats::default());

    let worker_count = scheduler_threads.get();
    #[cfg(feature = "perf-stats")]
    scheduler_stats
        .worker_count
        .store(worker_count, Ordering::Relaxed);

    let shared = Arc::new(Shared {
        next_id: AtomicUsize::new(1),
        lifecycle: AtomicU8::new(RUNNING),
        accepting_ingress: AtomicBool::new(true),
        ingress_tx,
        demand_tx,
        recovered_tx,
        registry: Mutex::new(HashMap::new()),
        terminal_tx: Mutex::new(Some(terminal_tx)),
        graceful_wake_tx,
        progress_wake_tx,
        ingress_pending: AtomicUsize::new(0),
        demand_pending: AtomicUsize::new(0),
        recovered_pending: AtomicUsize::new(0),
        active_worker_items: AtomicUsize::new(0),
        delivered_inflight: AtomicUsize::new(0),
        active_producer_sends: AtomicUsize::new(0),
        active_workers: AtomicUsize::new(worker_count),
        cancelled_emitted: AtomicBool::new(false),
        no_more_work_emitted: AtomicBool::new(false),
        status: status.clone(),
        #[cfg(feature = "perf-stats")]
        scheduler_stats: Arc::clone(&scheduler_stats),
        #[cfg(feature = "perf-stats")]
        consumer_stats: Mutex::new(Vec::new()),
    });

    let mut workers = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let shared_w = Arc::clone(&shared);
        let ingress_rx = ingress_rx.clone();
        let demand_rx = demand_rx.clone();
        let recovered_rx = recovered_rx.clone();
        let terminal_rx = terminal_rx.clone();
        let graceful_wake_rx = graceful_wake_rx.clone();
        let progress_wake_rx = progress_wake_rx.clone();
        workers.push(std::thread::spawn(move || {
            run_worker(
                shared_w,
                ingress_rx,
                demand_rx,
                recovered_rx,
                terminal_rx,
                graceful_wake_rx,
                progress_wake_rx,
            );
        }));
    }

    let runtime = Arc::new(Runtime { shared, workers });
    let id = alloc_feeder_id(&runtime.shared);
    Feeder { id, runtime }
}

fn alloc_feeder_id<T: Send + 'static, S: StatusSink>(shared: &Shared<T, S>) -> FeederId {
    FeederId(shared.next_id.fetch_add(1, Ordering::SeqCst))
}

fn alloc_receiver_id<T: Send + 'static, S: StatusSink>(shared: &Shared<T, S>) -> ReceiverId {
    ReceiverId(shared.next_id.fetch_add(1, Ordering::SeqCst))
}

fn lifecycle<T: Send + 'static, S: StatusSink>(shared: &Shared<T, S>) -> u8 {
    shared.lifecycle.load(Ordering::SeqCst)
}

fn spin_until<F: Fn() -> bool>(cond: F) {
    while !cond() {
        std::thread::yield_now();
    }
}

impl<T: Send + 'static, S: StatusSink> Shared<T, S> {
    fn signal_progress(&self) {
        let _ = self.progress_wake_tx.try_send(());
    }

    fn signal_completion_progress(&self) {
        if lifecycle(self) != RUNNING {
            self.signal_progress();
            self.try_complete();
        }
    }

    fn reserve_demand(&self, receiver: &Arc<ReceiverHandle<T>>, count: usize) -> bool {
        if count == 0 {
            return true;
        }
        receiver.outstanding.fetch_add(count, Ordering::AcqRel);
        self.demand_pending.fetch_add(count, Ordering::AcqRel);
        #[cfg(feature = "perf-stats")]
        {
            self.scheduler_stats
                .demand_commands
                .fetch_add(1, Ordering::Relaxed);
            self.scheduler_stats
                .demand_requested
                .fetch_add(count as u64, Ordering::Relaxed);
        }
        match self.demand_tx.send(DemandCommand::Request {
            receiver: Arc::clone(receiver),
            count,
        }) {
            Ok(()) => true,
            Err(_) => {
                receiver.outstanding.fetch_sub(count, Ordering::AcqRel);
                self.demand_pending.fetch_sub(count, Ordering::AcqRel);
                false
            }
        }
    }

    fn abandon_demand(&self, receiver: &ReceiverHandle<T>, count: usize) {
        if count == 0 {
            return;
        }
        receiver.outstanding.fetch_sub(count, Ordering::AcqRel);
        self.demand_pending.fetch_sub(count, Ordering::AcqRel);
        #[cfg(feature = "perf-stats")]
        self.scheduler_stats
            .demand_abandoned
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    fn enqueue_recovered(&self, item: T) {
        let state = lifecycle(self);
        if state == CANCELLED || state == NO_MORE_WORK {
            return;
        }
        self.recovered_pending.fetch_add(1, Ordering::AcqRel);
        #[cfg(feature = "perf-stats")]
        self.scheduler_stats
            .recovered_enqueued
            .fetch_add(1, Ordering::Relaxed);
        match self.recovered_tx.send(item) {
            Ok(()) => {
                self.signal_progress();
            }
            Err(_) => {
                self.recovered_pending.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    fn close_terminal(&self) {
        let _ = self.terminal_tx.lock().unwrap().take();
    }

    fn mark_all_receivers_closing(&self) {
        let mut registry = self.registry.lock().unwrap();
        for handle in registry.values() {
            handle.closing.store(true, Ordering::SeqCst);
        }
        registry.clear();
    }

    fn try_complete(&self) {
        if lifecycle(self) != SHUTTING_DOWN {
            return;
        }
        if self.ingress_pending.load(Ordering::Acquire) != 0 {
            return;
        }
        if self.demand_pending.load(Ordering::Acquire) != 0 {
            return;
        }
        if self.recovered_pending.load(Ordering::Acquire) != 0 {
            return;
        }
        if self.active_worker_items.load(Ordering::Acquire) != 0 {
            return;
        }
        if self.delivered_inflight.load(Ordering::Acquire) != 0 {
            return;
        }
        if self.active_producer_sends.load(Ordering::Acquire) != 0 {
            return;
        }
        if self
            .lifecycle
            .compare_exchange(
                SHUTTING_DOWN,
                NO_MORE_WORK,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }
        if self
            .no_more_work_emitted
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.mark_all_receivers_closing();
        self.status.no_more_work();
        self.close_terminal();
        self.signal_progress();
    }

    fn cancel_internal(&self) {
        let prev = self.lifecycle.swap(CANCELLED, Ordering::SeqCst);
        self.accepting_ingress.store(false, Ordering::SeqCst);
        if prev != CANCELLED
            && prev != NO_MORE_WORK
            && self
                .cancelled_emitted
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.status.cancelled();
        }
        self.mark_all_receivers_closing();
        self.close_terminal();
        self.signal_progress();
    }
}

impl<T: Send + 'static, S: StatusSink> Drop for Runtime<T, S> {
    fn drop(&mut self) {
        self.shared.cancel_internal();
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

fn worker_cancelled<T: Send + 'static, S: StatusSink>(shared: &Shared<T, S>) -> bool {
    lifecycle(shared) == CANCELLED
}

fn worker_shutting_down<T: Send + 'static, S: StatusSink>(shared: &Shared<T, S>) -> bool {
    let s = lifecycle(shared);
    s == SHUTTING_DOWN || s == NO_MORE_WORK
}

fn shutdown_idle<T: Send + 'static, S: StatusSink>(shared: &Shared<T, S>) -> bool {
    shared.ingress_pending.load(Ordering::Acquire) == 0
        && shared.recovered_pending.load(Ordering::Acquire) == 0
        && shared.active_worker_items.load(Ordering::Acquire) == 0
}

fn try_acquire_item<T: Send + 'static, S: StatusSink>(
    shared: &Shared<T, S>,
    receiver: &ReceiverHandle<T>,
    ingress_rx: &Receiver<T>,
    recovered_rx: &Receiver<T>,
) -> Option<T> {
    if worker_cancelled(shared) {
        return None;
    }
    if receiver.closing.load(Ordering::SeqCst) {
        return None;
    }
    match recovered_rx.try_recv() {
        Ok(item) => {
            shared.recovered_pending.fetch_sub(1, Ordering::AcqRel);
            shared.active_worker_items.fetch_add(1, Ordering::AcqRel);
            #[cfg(feature = "perf-stats")]
            shared
                .scheduler_stats
                .recovered_routed
                .fetch_add(1, Ordering::Relaxed);
            return Some(item);
        }
        Err(TryRecvError::Disconnected) => return None,
        Err(TryRecvError::Empty) => {}
    }
    match ingress_rx.try_recv() {
        Ok(item) => {
            shared.ingress_pending.fetch_sub(1, Ordering::AcqRel);
            shared.active_worker_items.fetch_add(1, Ordering::AcqRel);
            #[cfg(feature = "perf-stats")]
            shared
                .scheduler_stats
                .ingress_consumed
                .fetch_add(1, Ordering::Relaxed);
            Some(item)
        }
        Err(TryRecvError::Disconnected) => None,
        Err(TryRecvError::Empty) => None,
    }
}

fn acquire_item<T: Send + 'static, S: StatusSink>(
    shared: &Shared<T, S>,
    receiver: &ReceiverHandle<T>,
    ingress_rx: &Receiver<T>,
    recovered_rx: &Receiver<T>,
    terminal_rx: &Receiver<()>,
    graceful_wake_rx: &Receiver<()>,
    progress_wake_rx: &Receiver<()>,
) -> Option<T> {
    loop {
        if let Some(item) =
            try_acquire_item(shared, receiver, ingress_rx, recovered_rx)
        {
            return Some(item);
        }
        if worker_cancelled(shared) {
            return None;
        }
        if receiver.closing.load(Ordering::SeqCst) {
            return None;
        }
        if worker_shutting_down(shared) && shutdown_idle(shared) {
            return None;
        }
        crossbeam_channel::select! {
            recv(recovered_rx) -> msg => {
                match msg {
                    Ok(item) => {
                        shared.recovered_pending.fetch_sub(1, Ordering::AcqRel);
                        shared.active_worker_items.fetch_add(1, Ordering::AcqRel);
                        #[cfg(feature = "perf-stats")]
                        shared.scheduler_stats.recovered_routed.fetch_add(1, Ordering::Relaxed);
                        return Some(item);
                    }
                    Err(RecvError) => return None,
                }
            }
            recv(ingress_rx) -> msg => {
                match msg {
                    Ok(item) => {
                        shared.ingress_pending.fetch_sub(1, Ordering::AcqRel);
                        shared.active_worker_items.fetch_add(1, Ordering::AcqRel);
                        #[cfg(feature = "perf-stats")]
                        shared.scheduler_stats.ingress_consumed.fetch_add(1, Ordering::Relaxed);
                        return Some(item);
                    }
                    Err(RecvError) => return None,
                }
            }
            recv(terminal_rx) -> _ => return None,
            recv(graceful_wake_rx) -> _ => {
                shared.try_complete();
            }
            recv(progress_wake_rx) -> _ => {
                shared.try_complete();
            }
        }
    }
}

fn commit_fulfillment<T: Send + 'static, S: StatusSink>(shared: &Shared<T, S>, fulfilled: usize) {
    if fulfilled == 0 {
        return;
    }
    shared
        .demand_pending
        .fetch_sub(fulfilled, Ordering::AcqRel);
    shared
        .active_worker_items
        .fetch_sub(fulfilled, Ordering::AcqRel);
    #[cfg(feature = "perf-stats")]
    {
        shared
            .scheduler_stats
            .demand_fulfilled
            .fetch_add(fulfilled as u64, Ordering::Relaxed);
        shared
            .scheduler_stats
            .output_routed
            .fetch_add(fulfilled as u64, Ordering::Relaxed);
    }
    shared.signal_completion_progress();
}

fn send_fulfillment_burst<T: Send + 'static, S: StatusSink>(
    shared: &Shared<T, S>,
    receiver: &ReceiverHandle<T>,
    ingress_rx: &Receiver<T>,
    recovered_rx: &Receiver<T>,
    mut item: T,
    remaining: &mut usize,
) -> usize {
    if receiver.closing.load(Ordering::SeqCst) {
        shared.active_worker_items.fetch_sub(1, Ordering::AcqRel);
        let unsatisfied = *remaining;
        shared.abandon_demand(receiver, unsatisfied);
        if !worker_cancelled(shared) {
            shared.enqueue_recovered(item);
        }
        *remaining = 0;
        return 0;
    }
    receiver.active_sends.fetch_add(1, Ordering::AcqRel);
    if receiver.closing.load(Ordering::SeqCst) {
        receiver.active_sends.fetch_sub(1, Ordering::AcqRel);
        shared.active_worker_items.fetch_sub(1, Ordering::AcqRel);
        let unsatisfied = *remaining;
        shared.abandon_demand(receiver, unsatisfied);
        if !worker_cancelled(shared) {
            shared.enqueue_recovered(item);
        }
        *remaining = 0;
        return 0;
    }

    let mut fulfilled = 0usize;
    loop {
        shared.delivered_inflight.fetch_add(1, Ordering::AcqRel);
        match receiver.data_tx.send(item) {
            Ok(()) => {
                fulfilled += 1;
                *remaining -= 1;
                if *remaining == 0 {
                    break;
                }
                let Some(next) = try_acquire_item(shared, receiver, ingress_rx, recovered_rx)
                else {
                    break;
                };
                item = next;
            }
            Err(e) => {
                shared.delivered_inflight.fetch_sub(1, Ordering::AcqRel);
                shared.active_worker_items.fetch_sub(1, Ordering::AcqRel);
                shared.abandon_demand(receiver, 1);
                *remaining -= 1;
                if !worker_cancelled(shared) {
                    shared.enqueue_recovered(e.into_inner());
                }
                break;
            }
        }
    }
    receiver.active_sends.fetch_sub(1, Ordering::AcqRel);
    commit_fulfillment(shared, fulfilled);
    fulfilled
}

fn fulfill_demand<T: Send + 'static, S: StatusSink>(
    shared: &Shared<T, S>,
    receiver: Arc<ReceiverHandle<T>>,
    mut count: usize,
    ingress_rx: &Receiver<T>,
    recovered_rx: &Receiver<T>,
    terminal_rx: &Receiver<()>,
    graceful_wake_rx: &Receiver<()>,
    progress_wake_rx: &Receiver<()>,
) {
    while count > 0 {
        if worker_cancelled(shared) {
            shared.abandon_demand(&receiver, count);
            return;
        }
        if receiver.closing.load(Ordering::SeqCst) {
            shared.abandon_demand(&receiver, count);
            return;
        }
        if worker_shutting_down(shared) && shutdown_idle(shared) {
            shared.abandon_demand(&receiver, count);
            shared.try_complete();
            return;
        }
        let Some(item) = acquire_item(
            shared,
            &receiver,
            ingress_rx,
            recovered_rx,
            terminal_rx,
            graceful_wake_rx,
            progress_wake_rx,
        ) else {
            if worker_cancelled(shared) {
                shared.abandon_demand(&receiver, count);
                return;
            }
            if receiver.closing.load(Ordering::SeqCst) {
                shared.abandon_demand(&receiver, count);
                return;
            }
            if worker_shutting_down(shared) && shutdown_idle(shared) {
                shared.abandon_demand(&receiver, count);
                shared.try_complete();
            }
            return;
        };
        send_fulfillment_burst(shared, &receiver, ingress_rx, recovered_rx, item, &mut count);
    }
}

fn run_worker<T: Send + 'static, S: StatusSink>(
    shared: Arc<Shared<T, S>>,
    ingress_rx: Receiver<T>,
    demand_rx: Receiver<DemandCommand<T>>,
    recovered_rx: Receiver<T>,
    terminal_rx: Receiver<()>,
    graceful_wake_rx: Receiver<()>,
    progress_wake_rx: Receiver<()>,
) {
    loop {
        if worker_cancelled(&shared) {
            break;
        }
        let cmd = match demand_rx.try_recv() {
            Ok(cmd) => cmd,
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {
                crossbeam_channel::select! {
                    recv(demand_rx) -> d => {
                        match d {
                            Ok(cmd) => cmd,
                            Err(RecvError) => break,
                        }
                    }
                    recv(terminal_rx) -> _ => break,
                    recv(graceful_wake_rx) -> _ => {
                        shared.try_complete();
                        continue;
                    }
                    recv(progress_wake_rx) -> _ => {
                        shared.try_complete();
                        continue;
                    }
                }
            }
        };
        match cmd {
            DemandCommand::Request { receiver, count } => {
                fulfill_demand(
                    &shared,
                    receiver,
                    count,
                    &ingress_rx,
                    &recovered_rx,
                    &terminal_rx,
                    &graceful_wake_rx,
                    &progress_wake_rx,
                );
            }
        }
    }

    if shared.active_workers.fetch_sub(1, Ordering::SeqCst) == 1 {
        shared.status.scheduler_exited();
    }
}

impl<T: Send + 'static> Feeder<T, NoStatus> {
    pub fn builder() -> FeederBuilder<T> {
        FeederBuilder::default()
    }
}

impl<T: Send + 'static, S: StatusSink> Feeder<T, S> {
    pub fn id(&self) -> FeederId {
        self.id
    }

    pub fn tx(&self) -> Result<FeederTx<T, S>, AccessError> {
        let state = lifecycle(&self.runtime.shared);
        match state {
            CANCELLED => return Err(AccessError::Cancelled),
            NO_MORE_WORK => return Err(AccessError::NoMoreWork),
            RUNNING | SHUTTING_DOWN => {}
            _ => return Err(AccessError::SchedulerStopped),
        }
        Ok(FeederTx {
            runtime: Arc::clone(&self.runtime),
        })
    }

    pub fn rx(&self, low: NonZeroUsize, high: NonZeroUsize) -> Result<FeederRx<T, S>, AccessError> {
        let low_water = low.get();
        let high_water = high.get();
        if low_water >= high_water {
            return Err(AccessError::InvalidWaterMarks);
        }

        let state = lifecycle(&self.runtime.shared);
        match state {
            CANCELLED => return Err(AccessError::Cancelled),
            NO_MORE_WORK => return Err(AccessError::NoMoreWork),
            RUNNING | SHUTTING_DOWN => {}
            _ => return Err(AccessError::SchedulerStopped),
        }

        let receiver_id = alloc_receiver_id(&self.runtime.shared);
        let (data_tx, data_rx) = crossbeam_channel::unbounded();
        let handle = Arc::new(ReceiverHandle {
            _receiver_id: receiver_id,
            data_tx,
            closing: AtomicBool::new(false),
            active_sends: AtomicUsize::new(0),
            outstanding: AtomicUsize::new(0),
        });

        self.runtime
            .shared
            .registry
            .lock()
            .unwrap()
            .insert(receiver_id, Arc::clone(&handle));

        if !self.runtime.shared.reserve_demand(&handle, high_water) {
            return Err(AccessError::SchedulerStopped);
        }

        self.runtime.shared.status.receiver_registered(receiver_id);

        #[cfg(feature = "perf-stats")]
        let consumer_stats = Arc::new(stats::ConsumerStats::default());
        #[cfg(feature = "perf-stats")]
        self.runtime
            .shared
            .consumer_stats
            .lock()
            .unwrap()
            .push(Arc::clone(&consumer_stats));

        Ok(FeederRx {
            receiver_id,
            data_rx: Mutex::new(Some(data_rx)),
            handle: Arc::downgrade(&handle),
            low_water,
            high_water,
            shared: Arc::clone(&self.runtime.shared),
            lagging: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
            #[cfg(feature = "perf-stats")]
            consumer_stats,
        })
    }

    #[cfg(feature = "perf-stats")]
    pub fn stats_snapshot(&self) -> FeederStatsSnapshot {
        let shared = &self.runtime.shared;
        let mut consumers = ConsumerStatsSnapshot::default();
        for c in shared.consumer_stats.lock().unwrap().iter() {
            let s = c.snapshot();
            consumers.get_calls += s.get_calls;
            consumers.items_consumed += s.items_consumed;
            consumers.refill_messages += s.refill_messages;
        }
        FeederStatsSnapshot {
            scheduler: shared.scheduler_stats.snapshot_pending(
                shared.ingress_pending.load(Ordering::Relaxed),
                shared.demand_pending.load(Ordering::Relaxed),
                shared.recovered_pending.load(Ordering::Relaxed),
                shared.active_worker_items.load(Ordering::Relaxed),
                shared.delivered_inflight.load(Ordering::Relaxed),
            ),
            consumers,
        }
    }

    #[cfg(feature = "perf-stats")]
    pub fn reset_stats(&self) {
        self.runtime.shared.scheduler_stats.reset();
        for c in self.runtime.shared.consumer_stats.lock().unwrap().iter() {
            c.reset();
        }
    }

    pub fn graceful_shutdown(&self) {
        self.runtime
            .shared
            .accepting_ingress
            .store(false, Ordering::SeqCst);
        spin_until(|| {
            self.runtime
                .shared
                .active_producer_sends
                .load(Ordering::Acquire)
                == 0
        });
        if self
            .runtime
            .shared
            .lifecycle
            .compare_exchange(RUNNING, SHUTTING_DOWN, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.runtime.shared.status.graceful_shutdown_started();
            let _ = self.runtime.shared.graceful_wake_tx.send(());
            self.runtime.shared.try_complete();
        }
    }

    pub fn cancel(&self) {
        self.runtime.shared.cancel_internal();
    }
}

impl<T: Send + 'static, S: StatusSink> Clone for Feeder<T, S> {
    fn clone(&self) -> Self {
        Feeder {
            id: alloc_feeder_id(&self.runtime.shared),
            runtime: Arc::clone(&self.runtime),
        }
    }
}

impl<T: Send + 'static, S: StatusSink> Clone for FeederTx<T, S> {
    fn clone(&self) -> Self {
        FeederTx {
            runtime: Arc::clone(&self.runtime),
        }
    }
}

impl<T: Send + 'static, S: StatusSink> FeederTx<T, S> {
    pub fn send(&self, item: T) -> Result<(), SendError<T>> {
        let shared = &self.runtime.shared;
        let state = lifecycle(shared);
        if state != RUNNING || !shared.accepting_ingress.load(Ordering::SeqCst) {
            return Err(send_error_for_state(state, item, shared));
        }
        shared.active_producer_sends.fetch_add(1, Ordering::AcqRel);
        let state = lifecycle(shared);
        if state != RUNNING || !shared.accepting_ingress.load(Ordering::SeqCst) {
            shared.active_producer_sends.fetch_sub(1, Ordering::AcqRel);
            return Err(send_error_for_state(state, item, shared));
        }
        shared.ingress_pending.fetch_add(1, Ordering::AcqRel);
        #[cfg(feature = "perf-stats")]
        shared
            .scheduler_stats
            .ingress_accepted
            .fetch_add(1, Ordering::Relaxed);
        match shared.ingress_tx.send(item) {
            Ok(()) => {
                shared.active_producer_sends.fetch_sub(1, Ordering::AcqRel);
                Ok(())
            }
            Err(e) => {
                let unsent = e.into_inner();
                shared.ingress_pending.fetch_sub(1, Ordering::AcqRel);
                shared.active_producer_sends.fetch_sub(1, Ordering::AcqRel);
                Err(SendError::SchedulerStopped(unsent))
            }
        }
    }

    pub fn send_batch(&self, items: Vec<T>) -> Result<(), SendBatchError<T>> {
        if items.is_empty() {
            return Ok(());
        }
        let shared = &self.runtime.shared;
        let state = lifecycle(shared);
        if state != RUNNING || !shared.accepting_ingress.load(Ordering::SeqCst) {
            return Err(send_batch_error_for_state(state, items, shared));
        }
        shared.active_producer_sends.fetch_add(1, Ordering::AcqRel);
        let state = lifecycle(shared);
        if state != RUNNING || !shared.accepting_ingress.load(Ordering::SeqCst) {
            shared.active_producer_sends.fetch_sub(1, Ordering::AcqRel);
            return Err(send_batch_error_for_state(state, items, shared));
        }

        let item_count = items.len();
        shared
            .ingress_pending
            .fetch_add(item_count, Ordering::AcqRel);
        #[cfg(feature = "perf-stats")]
        shared
            .scheduler_stats
            .ingress_accepted
            .fetch_add(item_count as u64, Ordering::Relaxed);

        let mut iter = items.into_iter();
        while let Some(item) = iter.next() {
            if let Err(e) = shared.ingress_tx.send(item) {
                let mut unsent = Vec::new();
                unsent.push(e.into_inner());
                unsent.extend(iter);
                let unsent_count = unsent.len();
                shared
                    .ingress_pending
                    .fetch_sub(unsent_count, Ordering::AcqRel);
                shared.active_producer_sends.fetch_sub(1, Ordering::AcqRel);
                return Err(SendBatchError::SchedulerStopped(unsent));
            }
        }

        shared.active_producer_sends.fetch_sub(1, Ordering::AcqRel);
        Ok(())
    }
}

fn send_error_for_state<T: Send + 'static, S: StatusSink>(
    state: u8,
    item: T,
    shared: &Shared<T, S>,
) -> SendError<T> {
    match state {
        CANCELLED => SendError::Cancelled(item),
        NO_MORE_WORK => SendError::NoMoreWork(item),
        SHUTTING_DOWN | RUNNING if !shared.accepting_ingress.load(Ordering::SeqCst) => {
            SendError::IngressSealed(item)
        }
        SHUTTING_DOWN => SendError::IngressSealed(item),
        _ => SendError::SchedulerStopped(item),
    }
}

fn send_batch_error_for_state<T: Send + 'static, S: StatusSink>(
    state: u8,
    items: Vec<T>,
    shared: &Shared<T, S>,
) -> SendBatchError<T> {
    match state {
        CANCELLED => SendBatchError::Cancelled(items),
        NO_MORE_WORK => SendBatchError::NoMoreWork(items),
        SHUTTING_DOWN | RUNNING if !shared.accepting_ingress.load(Ordering::SeqCst) => {
            SendBatchError::IngressSealed(items)
        }
        SHUTTING_DOWN => SendBatchError::IngressSealed(items),
        _ => SendBatchError::SchedulerStopped(items),
    }
}

impl<T: Send + 'static, S: StatusSink> FeederRx<T, S> {
    pub fn id(&self) -> ReceiverId {
        self.receiver_id
    }

    pub fn get(&self, max: NonZeroUsize) -> Result<Vec<T>, GetError> {
        let max_usize = max.get();
        let mut guard = self.data_rx.lock().unwrap();
        let rx = match guard.as_mut() {
            Some(r) => r,
            None => return self.disconnected_error(),
        };

        let first = match rx.recv() {
            Ok(item) => item,
            Err(_) => return self.disconnected_error(),
        };

        let mut items = Vec::with_capacity(max_usize);
        items.push(first);
        while items.len() < max_usize {
            match rx.try_recv() {
                Ok(item) => items.push(item),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }

        self.finish_consume(max_usize, items.len(), items)
    }

    pub fn get_one(&self) -> Result<T, GetError> {
        let mut guard = self.data_rx.lock().unwrap();
        let rx = match guard.as_mut() {
            Some(r) => r,
            None => return Err(self.disconnected_get_error()),
        };

        let item = match rx.recv() {
            Ok(item) => item,
            Err(_) => return Err(self.disconnected_get_error()),
        };

        self.finish_consume_count(1, 1);
        Ok(item)
    }

    pub fn try_get(&self, max: NonZeroUsize) -> TryGet<T> {
        let max_usize = max.get();
        let mut guard = self.data_rx.lock().unwrap();
        let rx = match guard.as_mut() {
            Some(r) => r,
            None => return self.try_disconnected(),
        };

        let first = match rx.try_recv() {
            Ok(item) => item,
            Err(TryRecvError::Empty) => return self.try_empty(),
            Err(TryRecvError::Disconnected) => return self.try_disconnected(),
        };

        let mut items = vec![first];
        while items.len() < max_usize {
            match rx.try_recv() {
                Ok(item) => items.push(item),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }

        match self.finish_consume(max_usize, items.len(), items) {
            Ok(v) => TryGet::Items(v),
            Err(_) => self.try_disconnected(),
        }
    }

    pub fn try_get_one(&self) -> TryGetOne<T> {
        let mut guard = self.data_rx.lock().unwrap();
        let rx = match guard.as_mut() {
            Some(r) => r,
            None => return self.try_disconnected_one(),
        };

        let item = match rx.try_recv() {
            Ok(item) => item,
            Err(TryRecvError::Empty) => return self.try_empty_one(),
            Err(TryRecvError::Disconnected) => return self.try_disconnected_one(),
        };

        self.finish_consume_count(1, 1);
        TryGetOne::Item(item)
    }

    pub fn cancel(&self) {
        self.notify_drop();
    }

    fn finish_consume(
        &self,
        requested: usize,
        consumed: usize,
        items: Vec<T>,
    ) -> Result<Vec<T>, GetError> {
        self.finish_consume_count(requested, consumed);
        Ok(items)
    }

    fn finish_consume_count(&self, requested: usize, consumed: usize) {
        self.shared
            .delivered_inflight
            .fetch_sub(consumed, Ordering::AcqRel);
        if let Some(handle) = self.handle.upgrade() {
            handle.outstanding.fetch_sub(consumed, Ordering::AcqRel);
        }
        #[cfg(feature = "perf-stats")]
        {
            self.consumer_stats
                .get_calls
                .fetch_add(1, Ordering::Relaxed);
            self.consumer_stats
                .items_consumed
                .fetch_add(consumed as u64, Ordering::Relaxed);
        }
        self.maybe_request_refill();
        if consumed < requested || self.lagging.load(Ordering::Relaxed) {
            self.update_lag(requested, consumed);
        }
        self.shared.signal_completion_progress();
    }

    fn maybe_request_refill(&self) {
        let Some(handle) = self.handle.upgrade() else {
            return;
        };
        let outstanding = handle.outstanding.load(Ordering::Acquire);
        if outstanding >= self.low_water {
            return;
        }
        let count = self.high_water.saturating_sub(outstanding);
        if count == 0 {
            return;
        }
        #[cfg(feature = "perf-stats")]
        self.consumer_stats
            .refill_messages
            .fetch_add(1, Ordering::Relaxed);
        let _ = self.shared.reserve_demand(&handle, count);
    }

    fn disconnected_get_error(&self) -> GetError {
        if lifecycle(&self.shared) == CANCELLED {
            GetError::Cancelled
        } else {
            GetError::NoMoreWork
        }
    }

    fn disconnected_error(&self) -> Result<Vec<T>, GetError> {
        Err(self.disconnected_get_error())
    }

    fn try_empty(&self) -> TryGet<T> {
        match lifecycle(&self.shared) {
            RUNNING => TryGet::Empty,
            SHUTTING_DOWN => TryGet::InShutdown,
            CANCELLED => TryGet::Cancelled,
            NO_MORE_WORK => TryGet::NoMoreWork,
            _ => TryGet::NoMoreWork,
        }
    }

    fn try_disconnected(&self) -> TryGet<T> {
        if lifecycle(&self.shared) == CANCELLED {
            TryGet::Cancelled
        } else {
            TryGet::NoMoreWork
        }
    }

    fn try_empty_one(&self) -> TryGetOne<T> {
        match lifecycle(&self.shared) {
            RUNNING => TryGetOne::Empty,
            SHUTTING_DOWN => TryGetOne::InShutdown,
            CANCELLED => TryGetOne::Cancelled,
            NO_MORE_WORK => TryGetOne::NoMoreWork,
            _ => TryGetOne::NoMoreWork,
        }
    }

    fn try_disconnected_one(&self) -> TryGetOne<T> {
        if lifecycle(&self.shared) == CANCELLED {
            TryGetOne::Cancelled
        } else {
            TryGetOne::NoMoreWork
        }
    }

    fn update_lag(&self, requested: usize, received: usize) {
        let state = lifecycle(&self.shared);
        if state != RUNNING {
            return;
        }
        if received < requested {
            if !self.lagging.swap(true, Ordering::SeqCst) {
                self.shared
                    .status
                    .lagging(self.receiver_id, requested, received);
            }
        } else if self.lagging.swap(false, Ordering::SeqCst) {
            self.shared.status.recovered(self.receiver_id);
        }
    }

    fn notify_drop(&self) {
        if self.dropped.swap(true, Ordering::SeqCst) {
            return;
        }
        let Some(handle) = self.handle.upgrade() else {
            return;
        };
        handle.closing.store(true, Ordering::SeqCst);
        self.shared
            .registry
            .lock()
            .unwrap()
            .remove(&self.receiver_id);
        spin_until(|| handle.active_sends.load(Ordering::Acquire) == 0);
        let data_rx = self.data_rx.lock().unwrap().take();
        let mut recovered = 0usize;
        if let Some(data_rx) = data_rx {
            for item in data_rx.try_iter() {
                self.shared
                    .delivered_inflight
                    .fetch_sub(1, Ordering::SeqCst);
                handle.outstanding.fetch_sub(1, Ordering::AcqRel);
                if lifecycle(&self.shared) != CANCELLED {
                    self.shared.enqueue_recovered(item);
                }
                recovered += 1;
            }
        }
        #[cfg(feature = "perf-stats")]
        self.shared
            .scheduler_stats
            .receiver_drop_recovered
            .fetch_add(recovered as u64, Ordering::Relaxed);
        self.shared
            .status
            .receiver_dropped(self.receiver_id, recovered);
        self.shared.signal_progress();
        self.shared.try_complete();
    }
}

impl<T: Send + 'static, S: StatusSink> Drop for FeederRx<T, S> {
    fn drop(&mut self) {
        self.notify_drop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::time::Duration;

    fn make_feeder<T: Send + 'static>() -> Feeder<T, NoStatus> {
        Feeder::<T, NoStatus>::builder().build()
    }

    fn water(low: usize, high: usize) -> (NonZeroUsize, NonZeroUsize) {
        (
            NonZeroUsize::new(low).unwrap(),
            NonZeroUsize::new(high).unwrap(),
        )
    }

    #[test]
    fn rx_rejects_invalid_water_marks() {
        let feeder = make_feeder::<i32>();
        assert!(matches!(
            feeder.rx(water(2, 2).0, water(2, 2).1),
            Err(AccessError::InvalidWaterMarks)
        ));
        assert!(matches!(
            feeder.rx(water(3, 2).0, water(3, 2).1),
            Err(AccessError::InvalidWaterMarks)
        ));
    }

    #[test]
    fn refill_signals_only_below_low_water() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(2, 4).0, water(2, 4).1).unwrap();
        let tx = feeder.tx().unwrap();
        for i in 0..4 {
            tx.send(i).unwrap();
        }
        std::thread::sleep(Duration::from_millis(50));
        let batch = rx.get(NonZeroUsize::new(2).unwrap()).unwrap();
        assert_eq!(batch.len(), 2);
        std::thread::sleep(Duration::from_millis(50));
        let one = rx.get_one().unwrap();
        assert_eq!(one, 2);
    }

    #[test]
    fn tx_returns_sender_while_running() {
        let feeder = make_feeder::<i32>();
        assert!(feeder.tx().is_ok());
    }

    #[test]
    fn send_returns_ingress_sealed_after_graceful_shutdown() {
        let feeder = make_feeder::<i32>();
        let _rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(1).unwrap();
        feeder.graceful_shutdown();
        assert!(matches!(tx.send(2), Err(SendError::IngressSealed(2))));
    }

    #[test]
    fn send_after_graceful_shutdown_returns_original_item() {
        let feeder = make_feeder::<i32>();
        let _rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(1).unwrap();
        feeder.graceful_shutdown();
        for v in [7i32, 8, 9] {
            match tx.send(v) {
                Err(SendError::IngressSealed(got)) => assert_eq!(got, v),
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn send_batch_delivers_items() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(1, 4).0, water(1, 4).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send_batch(vec![1, 2, 3]).unwrap();
        assert_eq!(rx.get_one().unwrap(), 1);
        assert_eq!(rx.get_one().unwrap(), 2);
        assert_eq!(rx.get_one().unwrap(), 3);
    }

    #[test]
    fn send_batch_after_graceful_shutdown_returns_original_items() {
        let feeder = make_feeder::<i32>();
        let _rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(1).unwrap();
        feeder.graceful_shutdown();
        match tx.send_batch(vec![7, 8, 9]) {
            Err(SendBatchError::IngressSealed(items)) => assert_eq!(items, vec![7, 8, 9]),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn immediate_no_more_work_on_empty_graceful_shutdown() {
        let feeder = make_feeder::<i32>();
        feeder.graceful_shutdown();
        assert!(matches!(
            feeder.rx(water(1, 2).0, water(1, 2).1),
            Err(AccessError::NoMoreWork)
        ));
    }

    #[test]
    fn rx_allowed_during_graceful_shutdown() {
        let feeder = make_feeder::<i32>();
        let tx = feeder.tx().unwrap();
        tx.send(1).unwrap();
        feeder.graceful_shutdown();
        assert!(feeder.rx(water(1, 2).0, water(1, 2).1).is_ok());
    }

    #[test]
    fn rx_rejected_after_cancel() {
        let feeder = make_feeder::<i32>();
        feeder.cancel();
        std::thread::sleep(Duration::from_millis(50));
        assert!(matches!(
            feeder.rx(water(1, 2).0, water(1, 2).1),
            Err(AccessError::Cancelled)
        ));
    }

    #[test]
    fn get_returns_non_empty_vec_up_to_max() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        for i in 0..5 {
            tx.send(i).unwrap();
        }
        let batch = rx.get(NonZeroUsize::new(3).unwrap()).unwrap();
        assert!(!batch.is_empty());
        assert!(batch.len() <= 3);
    }

    #[test]
    fn get_one_returns_single_item() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(42).unwrap();
        assert_eq!(rx.get_one().unwrap(), 42);
    }

    #[test]
    fn try_get_empty_while_running() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        assert!(matches!(
            rx.try_get(NonZeroUsize::new(3).unwrap()),
            TryGet::Empty
        ));
    }

    #[test]
    fn try_get_in_shutdown_when_empty_and_shutdown_started() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(1).unwrap();
        let _ = rx.get_one().unwrap();
        feeder.graceful_shutdown();
        assert!(matches!(
            rx.try_get(NonZeroUsize::new(3).unwrap()),
            TryGet::InShutdown
        ));
    }

    #[test]
    fn try_get_no_more_work_after_final_drain() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(1).unwrap();
        let _ = rx.get_one().unwrap();
        feeder.graceful_shutdown();
        drop(tx);
        loop {
            match rx.try_get_one() {
                TryGetOne::Item(_) => continue,
                TryGetOne::NoMoreWork => break,
                TryGetOne::InShutdown | TryGetOne::Empty => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn cancel_wakes_blocking_get() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let feeder2 = feeder.clone();
        let handle = std::thread::spawn(move || rx.get_one());
        std::thread::sleep(Duration::from_millis(50));
        feeder2.cancel();
        assert_eq!(handle.join().unwrap(), Err(GetError::Cancelled));
    }

    #[test]
    fn receiver_refills_only_consumed_count() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        assert_eq!(rx.get_one().unwrap(), 1);
        assert_eq!(rx.get_one().unwrap(), 2);
        tx.send(4).unwrap();
        let one = rx.get_one().unwrap();
        assert_eq!(one, 3);
    }

    #[test]
    fn recovered_work_is_prioritized_before_ingress() {
        let feeder = make_feeder::<i32>();
        let rx1 = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(100).unwrap();
        let _ = rx1.get_one().unwrap();
        tx.send(200).unwrap();
        drop(rx1);
        std::thread::sleep(Duration::from_millis(50));
        let rx2 = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        tx.send(300).unwrap();
        let first = rx2.get_one().unwrap();
        assert_eq!(first, 200);
    }

    #[test]
    fn receiver_drop_recovers_buffered_items_without_loss() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(2, 5).0, water(2, 5).1).unwrap();
        let tx = feeder.tx().unwrap();
        for i in 0..10 {
            tx.send(i).unwrap();
        }
        std::thread::sleep(Duration::from_millis(100));
        drop(rx);
        std::thread::sleep(Duration::from_millis(100));
        let rx2 = feeder.rx(water(5, 10).0, water(5, 10).1).unwrap();
        let mut sum = 0i32;
        for _ in 0..20 {
            match rx2.try_get_one() {
                TryGetOne::Item(v) => sum += v,
                TryGetOne::Empty => std::thread::sleep(Duration::from_millis(10)),
                TryGetOne::NoMoreWork => break,
                _ => {}
            }
        }
        assert_eq!(sum, (0..10).sum::<i32>());
    }

    #[test]
    fn multi_worker_preserves_total_count_and_checksum() {
        let feeder = Feeder::<u64>::builder()
            .scheduler_threads(NonZeroUsize::new(4).unwrap())
            .build();
        let rx = feeder.rx(water(1, 8).0, water(1, 8).1).unwrap();
        let n_producers = 4usize;
        let per_producer = 500usize;
        let mut handles = Vec::new();
        for p in 0..n_producers {
            let tx = feeder.tx().unwrap();
            handles.push(std::thread::spawn(move || {
                let base = (p * per_producer) as u64;
                for i in 0..per_producer {
                    tx.send(base + i as u64).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        feeder.graceful_shutdown();
        let expected_sum = (0..(n_producers * per_producer) as u64).sum::<u64>();
        let mut sum = 0u64;
        loop {
            match rx.try_get(NonZeroUsize::new(32).unwrap()) {
                TryGet::Items(batch) => {
                    for v in batch {
                        sum += v;
                    }
                }
                TryGet::Empty | TryGet::InShutdown => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                TryGet::NoMoreWork => break,
                TryGet::Cancelled => panic!("cancelled"),
            }
        }
        assert_eq!(sum, expected_sum);
    }

    #[test]
    fn queued_demand_for_dropped_receiver_is_abandoned() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(1, 100).0, water(1, 100).1).unwrap();
        let demand_before = feeder.runtime.shared.demand_pending.load(Ordering::Acquire);
        assert!(demand_before >= 100);
        drop(rx);
        std::thread::sleep(Duration::from_millis(100));
        let demand_after = feeder.runtime.shared.demand_pending.load(Ordering::Acquire);
        assert!(demand_after < demand_before);
    }

    #[test]
    fn lifecycle_events_emitted_once() {
        let (feeder, status_rx) = Feeder::<i32>::builder().build_with_status();
        let rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(1).unwrap();
        let _ = rx.get_one().unwrap();
        feeder.graceful_shutdown();
        drop(tx);
        loop {
            match rx.try_get_one() {
                TryGetOne::Item(_) => continue,
                TryGetOne::NoMoreWork => break,
                _ => std::thread::sleep(Duration::from_millis(10)),
            }
        }
        drop(rx);
        drop(feeder);
        let mut graceful = 0;
        let mut no_more = 0;
        let mut cancelled = 0;
        let mut exited = 0;
        while let Ok(ev) = status_rx.try_recv() {
            match ev {
                StatusEvent::GracefulShutdownStarted => graceful += 1,
                StatusEvent::NoMoreWork => no_more += 1,
                StatusEvent::Cancelled => cancelled += 1,
                StatusEvent::SchedulerExited => exited += 1,
                _ => {}
            }
        }
        assert_eq!(graceful, 1);
        assert_eq!(no_more, 1);
        assert_eq!(cancelled, 0);
        assert_eq!(exited, 1);
    }

    #[test]
    fn refill_credits_do_not_over_request_under_concurrent_fulfillment() {
        let feeder = Feeder::<i32>::builder()
            .scheduler_threads(NonZeroUsize::new(4).unwrap())
            .build();
        let rx = feeder.rx(water(2, 10).0, water(2, 10).1).unwrap();
        let tx = feeder.tx().unwrap();
        for i in 0..20 {
            tx.send(i).unwrap();
        }
        std::thread::sleep(Duration::from_millis(100));
        if let Some(handle) = rx.handle.upgrade() {
            let outstanding = handle.outstanding.load(Ordering::SeqCst);
            assert!(outstanding <= 10, "outstanding={outstanding}");
        }
        let _ = rx.get(NonZeroUsize::new(5).unwrap());
        if let Some(handle) = rx.handle.upgrade() {
            let outstanding = handle.outstanding.load(Ordering::SeqCst);
            assert!(outstanding <= 10, "outstanding after consume={outstanding}");
        }
    }

    #[test]
    fn no_more_work_waits_for_in_flight_items() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(water(1, 3).0, water(1, 3).1).unwrap();
        let tx = feeder.tx().unwrap();
        for i in 0..6 {
            tx.send(i).unwrap();
        }
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        let rx_thread = std::thread::spawn(move || {
            b.wait();
            let batch = rx.get(NonZeroUsize::new(3).unwrap()).unwrap();
            (batch, rx)
        });
        barrier.wait();
        feeder.graceful_shutdown();
        drop(tx);
        let (batch, rx) = rx_thread.join().unwrap();
        assert_eq!(batch.len(), 3);
        assert!(feeder.rx(water(1, 2).0, water(1, 2).1).is_ok());
        loop {
            match rx.try_get_one() {
                TryGetOne::Item(_) => {}
                TryGetOne::Empty | TryGetOne::InShutdown => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                TryGetOne::NoMoreWork | TryGetOne::Cancelled => break,
            }
        }
        drop(rx);
        std::thread::sleep(Duration::from_millis(100));
        assert!(matches!(
            feeder.rx(water(1, 2).0, water(1, 2).1),
            Err(AccessError::NoMoreWork)
        ));
    }

    #[test]
    fn status_emits_lag_once_then_recovered_once() {
        let (feeder, status_rx) = Feeder::<i32>::builder().build_with_status();
        let rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(1).unwrap();
        let _ = rx.get(NonZeroUsize::new(3).unwrap()).unwrap();
        let mut lag_count = 0;
        let mut recovered_count = 0;
        while let Ok(ev) = status_rx.try_recv() {
            match ev {
                StatusEvent::Lagging { .. } => lag_count += 1,
                StatusEvent::Recovered { .. } => recovered_count += 1,
                _ => {}
            }
        }
        assert_eq!(lag_count, 1);
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        tx.send(4).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let _ = rx.get_one().unwrap();
        while let Ok(ev) = status_rx.try_recv() {
            match ev {
                StatusEvent::Lagging { .. } => lag_count += 1,
                StatusEvent::Recovered { .. } => recovered_count += 1,
                _ => {}
            }
        }
        assert_eq!(recovered_count, 1);
        drop(tx);
        drop(feeder);
    }

    #[test]
    fn status_emits_lifecycle_events() {
        let (feeder, status_rx) = Feeder::<i32>::builder().build_with_status();
        let rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(1).unwrap();
        let _ = rx.get_one().unwrap();
        feeder.graceful_shutdown();
        drop(tx);
        loop {
            match rx.try_get_one() {
                TryGetOne::Item(_) => continue,
                TryGetOne::NoMoreWork => break,
                _ => std::thread::sleep(Duration::from_millis(10)),
            }
        }
        drop(rx);
        drop(feeder);
        let mut events = Vec::new();
        while let Ok(ev) = status_rx.try_recv() {
            events.push(ev);
        }
        assert!(events
            .iter()
            .any(|e| matches!(e, StatusEvent::ReceiverRegistered { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, StatusEvent::GracefulShutdownStarted)));
        assert!(events.iter().any(|e| matches!(e, StatusEvent::NoMoreWork)));
        assert!(events
            .iter()
            .any(|e| matches!(e, StatusEvent::SchedulerExited)));
    }

    #[test]
    fn feeder_clone_gets_new_monotonic_id() {
        let feeder = make_feeder::<i32>();
        let id1 = feeder.id();
        let feeder2 = feeder.clone();
        let id2 = feeder2.id();
        assert_ne!(id1, id2);
        assert!(id2.0 > id1.0);
    }

    #[test]
    fn receiver_registrations_get_monotonic_ids() {
        let feeder = make_feeder::<i32>();
        let rx1 = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        let rx2 = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
        assert!(rx2.id().0 > rx1.id().0);
    }

    #[cfg(feature = "perf-stats")]
    mod perf_stats_tests {
        use super::*;

        #[test]
        fn reset_stats_clears_counters() {
            let feeder = make_feeder::<i32>();
            let rx = feeder.rx(water(1, 2).0, water(1, 2).1).unwrap();
            let tx = feeder.tx().unwrap();
            tx.send(1).unwrap();
            let _ = rx.get_one().unwrap();
            let snap = feeder.stats_snapshot();
            assert!(snap.scheduler.output_routed > 0 || snap.consumers.items_consumed > 0);
            feeder.reset_stats();
            let cleared = feeder.stats_snapshot();
            assert_eq!(cleared.scheduler.output_routed, 0);
            assert_eq!(cleared.consumers.items_consumed, 0);
            assert_eq!(cleared.consumers.refill_messages, 0);
        }

        #[test]
        fn routed_items_match_consumed() {
            let feeder = make_feeder::<i32>();
            let rx = feeder.rx(water(1, 4).0, water(1, 4).1).unwrap();
            let tx = feeder.tx().unwrap();
            for i in 0..4 {
                tx.send(i).unwrap();
            }
            std::thread::sleep(Duration::from_millis(50));
            let batch = rx.get(NonZeroUsize::new(4).unwrap()).unwrap();
            assert_eq!(batch.len(), 4);
            let snap = feeder.stats_snapshot();
            assert_eq!(snap.consumers.items_consumed, 4);
            assert_eq!(snap.scheduler.output_routed, 4);
        }
    }
}
