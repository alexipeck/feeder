use crossbeam_channel::{Receiver, RecvError, Sender, TryRecvError};
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use thiserror::Error;

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
    ReceiverRegistered { receiver_id: ReceiverId },
    ReceiverDropped { receiver_id: ReceiverId, recovered: usize },
    GracefulShutdownStarted,
    Cancelled,
    NoMoreWork,
    SchedulerExited,
    Lagging {
        receiver_id: ReceiverId,
        requested: usize,
        received: usize,
    },
    Recovered { receiver_id: ReceiverId },
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
        let _ = self.tx.send(StatusEvent::ReceiverRegistered { receiver_id });
    }
    fn receiver_dropped(&self, receiver_id: ReceiverId, recovered: usize) {
        let _ = self
            .tx
            .send(StatusEvent::ReceiverDropped {
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
    _marker: PhantomData<T>,
}

impl<T: Send + 'static> FeederBuilder<T> {
    pub fn build(self) -> Feeder<T, NoStatus> {
        build_feeder(NoStatus)
    }

    pub fn build_with_status(self) -> (Feeder<T, ChannelStatus>, Receiver<StatusEvent>) {
        let (status_tx, status_rx) = crossbeam_channel::unbounded();
        let feeder = build_feeder(ChannelStatus::new(status_tx));
        (feeder, status_rx)
    }
}

pub struct Feeder<T: Send + 'static, S: StatusSink = NoStatus> {
    id: FeederId,
    shared: Arc<Shared<T, S>>,
}

pub struct FeederRx<T: Send + 'static, S: StatusSink = NoStatus> {
    receiver_id: ReceiverId,
    data_rx: Mutex<Option<Receiver<T>>>,
    demand_tx: Sender<DemandCommand>,
    in_flight: Arc<AtomicUsize>,
    shared: Arc<Shared<T, S>>,
    lagging: AtomicBool,
    dropped: AtomicBool,
}

struct Shared<T: Send + 'static, S: StatusSink> {
    next_id: AtomicUsize,
    lifecycle: AtomicU8,
    ingress: Mutex<Option<Sender<T>>>,
    demand_tx: Sender<DemandCommand>,
    control_tx: Sender<ControlCommand<T>>,
    scheduler: Mutex<Option<JoinHandle<()>>>,
    status: S,
}

struct ReceiverState<T> {
    data_tx: Sender<T>,
    in_flight: Arc<AtomicUsize>,
    active: bool,
}

#[derive(Debug)]
enum DemandCommand {
    Request {
        receiver_id: ReceiverId,
        count: usize,
    },
}

#[derive(Debug)]
enum ControlCommand<T> {
    RegisterReceiver {
        receiver_id: ReceiverId,
        data_tx: Sender<T>,
        in_flight: Arc<AtomicUsize>,
        initial_prefetch: usize,
    },
    ReceiverDropped {
        receiver_id: ReceiverId,
        data_rx: Receiver<T>,
    },
    GracefulShutdown,
    Cancel,
}

struct HeadDemand {
    receiver_id: ReceiverId,
    remaining: usize,
}

fn build_feeder<T: Send + 'static, S: StatusSink>(status: S) -> Feeder<T, S> {
    let (ingress_tx, ingress_rx) = crossbeam_channel::unbounded();
    let (demand_tx, demand_rx) = crossbeam_channel::unbounded();
    let (control_tx, control_rx) = crossbeam_channel::unbounded();

    let shared = Arc::new(Shared {
        next_id: AtomicUsize::new(1),
        lifecycle: AtomicU8::new(RUNNING),
        ingress: Mutex::new(Some(ingress_tx)),
        demand_tx: demand_tx.clone(),
        control_tx: control_tx.clone(),
        scheduler: Mutex::new(None),
        status: status.clone(),
    });

    let shared_scheduler = Arc::clone(&shared);
    let handle = std::thread::spawn(move || {
        run_scheduler(
            ingress_rx,
            demand_rx,
            control_rx,
            shared_scheduler,
        );
    });
    *shared.scheduler.lock().unwrap() = Some(handle);

    let id = alloc_feeder_id(&shared);
    Feeder { id, shared }
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

fn receiver_ready<T>(receivers: &HashMap<ReceiverId, ReceiverState<T>>, id: ReceiverId) -> bool {
    receivers.get(&id).is_some_and(|s| s.active)
}

fn queue_demand<T>(
    head: &mut Option<HeadDemand>,
    pending: &mut VecDeque<HeadDemand>,
    receivers: &HashMap<ReceiverId, ReceiverState<T>>,
    receiver_id: ReceiverId,
    count: usize,
) {
    if count == 0 {
        return;
    }
    let demand = HeadDemand {
        receiver_id,
        remaining: count,
    };
    if head.is_some() {
        pending.push_back(demand);
        return;
    }
    if receiver_ready(receivers, receiver_id) {
        *head = Some(demand);
    } else {
        pending.push_back(demand);
    }
}

fn advance_head_demand<T>(
    head: &mut Option<HeadDemand>,
    pending: &mut VecDeque<HeadDemand>,
    receivers: &HashMap<ReceiverId, ReceiverState<T>>,
) {
    if let Some(current) = head.as_ref() {
        if receiver_ready(receivers, current.receiver_id) {
            return;
        }
        head.take();
    }
    while let Some(demand) = pending.pop_front() {
        if receiver_ready(receivers, demand.receiver_id) {
            *head = Some(demand);
            return;
        }
    }
}

fn fulfill_from_recovered<T>(
    head: &mut HeadDemand,
    receivers: &mut HashMap<ReceiverId, ReceiverState<T>>,
    recovered: &mut VecDeque<T>,
) -> bool {
    let rid = head.receiver_id;
    let Some(state) = receivers.get_mut(&rid) else {
        return false;
    };
    if !state.active {
        return false;
    }
    while head.remaining > 0 {
        let Some(item) = recovered.pop_front() else {
            break;
        };
        if state.data_tx.send(item).is_ok() {
            state.in_flight.fetch_add(1, Ordering::SeqCst);
            head.remaining -= 1;
        } else {
            return false;
        }
    }
    true
}

fn drop_receiver_senders<T>(receivers: &mut HashMap<ReceiverId, ReceiverState<T>>) {
    for (_, state) in receivers.iter_mut() {
        state.active = false;
        let (tx, _) = crossbeam_channel::unbounded();
        drop(std::mem::replace(&mut state.data_tx, tx));
    }
}

fn run_scheduler<T: Send + 'static, S: StatusSink>(
    ingress_rx: Receiver<T>,
    demand_rx: Receiver<DemandCommand>,
    control_rx: Receiver<ControlCommand<T>>,
    shared: Arc<Shared<T, S>>,
) {
    let mut receivers: HashMap<ReceiverId, ReceiverState<T>> = HashMap::new();
    let mut recovered: VecDeque<T> = VecDeque::new();
    let mut head_demand: Option<HeadDemand> = None;
    let mut pending_demands: VecDeque<HeadDemand> = VecDeque::new();
    let mut ingress_closed = false;
    let mut cancelled = false;
    let mut graceful_shutdown = false;
    let mut demand_closed = false;

    let handle_control = |cmd: Result<ControlCommand<T>, RecvError>,
                            receivers: &mut HashMap<ReceiverId, ReceiverState<T>>,
                            recovered: &mut VecDeque<T>,
                            head_demand: &mut Option<HeadDemand>,
                            pending_demands: &mut VecDeque<HeadDemand>,
                            graceful_shutdown: &mut bool,
                            cancelled: &mut bool|
     -> bool {
        match cmd {
            Ok(ControlCommand::RegisterReceiver {
                receiver_id,
                data_tx,
                in_flight,
                initial_prefetch: _,
            }) => {
                receivers.insert(
                    receiver_id,
                    ReceiverState {
                        data_tx,
                        in_flight,
                        active: true,
                    },
                );
                advance_head_demand(head_demand, pending_demands, receivers);
            }
            Ok(ControlCommand::ReceiverDropped { receiver_id, data_rx }) => {
                recover_receiver(
                    receiver_id,
                    data_rx,
                    receivers,
                    recovered,
                    &shared.status,
                );
            }
            Ok(ControlCommand::GracefulShutdown) => {
                *graceful_shutdown = true;
            }
            Ok(ControlCommand::Cancel) => {
                *cancelled = true;
                shared.lifecycle.store(CANCELLED, Ordering::SeqCst);
                shared.status.cancelled();
                drop_receiver_senders(receivers);
                receivers.clear();
                recovered.clear();
                head_demand.take();
                pending_demands.clear();
                return true;
            }
            Err(_) => {}
        }
        false
    };

    'outer: loop {
        if cancelled {
            break;
        }

        if head_demand.is_none() && !demand_closed {
            match demand_rx.try_recv() {
                Ok(DemandCommand::Request {
                    receiver_id,
                    count,
                }) => {
                    queue_demand(
                        &mut head_demand,
                        &mut pending_demands,
                        &receivers,
                        receiver_id,
                        count,
                    );
                }
                Err(TryRecvError::Disconnected) => demand_closed = true,
                Err(TryRecvError::Empty) => {}
            }
        }

        if let Some(head) = &mut head_demand {
            if !receiver_ready(&receivers, head.receiver_id) {
                let stale = head_demand.take().unwrap();
                pending_demands.push_back(stale);
                advance_head_demand(&mut head_demand, &mut pending_demands, &receivers);
                continue;
            }

            if let Some(head) = head_demand.as_mut() {
                fulfill_from_recovered(head, &mut receivers, &mut recovered);
            }

            if let Some(head) = head_demand.as_ref() {
                if head.remaining == 0 {
                    head_demand.take();
                    advance_head_demand(&mut head_demand, &mut pending_demands, &receivers);
                    continue;
                }
            } else {
                continue;
            }

            let rid = head_demand.as_ref().unwrap().receiver_id;

            if ingress_closed && recovered.is_empty() {
                if head_demand.as_ref().is_some_and(|h| h.remaining > 0) {
                    head_demand.take();
                    advance_head_demand(&mut head_demand, &mut pending_demands, &receivers);
                    continue;
                }
                let no_demand = head_demand.is_none() && pending_demands.is_empty();
                if try_finish_no_more_work(
                    &shared,
                    graceful_shutdown,
                    ingress_closed,
                    &recovered,
                    no_demand,
                    &receivers,
                ) {
                    break 'outer;
                }
            }

            crossbeam_channel::select! {
                recv(ingress_rx) -> msg => {
                    match msg {
                        Ok(item) => {
                            if let Some(state) = receivers.get_mut(&rid) {
                                if state.active && state.data_tx.send(item).is_ok() {
                                    state.in_flight.fetch_add(1, Ordering::SeqCst);
                                    if let Some(h) = &mut head_demand {
                                        h.remaining = h.remaining.saturating_sub(1);
                                        if h.remaining == 0 {
                                            head_demand.take();
                                            advance_head_demand(
                                                &mut head_demand,
                                                &mut pending_demands,
                                                &receivers,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Err(RecvError) => {
                            ingress_closed = true;
                        }
                    }
                }
                recv(control_rx) -> cmd => {
                    if handle_control(
                        cmd,
                        &mut receivers,
                        &mut recovered,
                        &mut head_demand,
                        &mut pending_demands,
                        &mut graceful_shutdown,
                        &mut cancelled,
                    ) {
                        break 'outer;
                    }
                }
            }
            continue;
        }

        let no_demand = head_demand.is_none() && pending_demands.is_empty();
        if try_finish_no_more_work(
            &shared,
            graceful_shutdown,
            ingress_closed,
            &recovered,
            no_demand,
            &receivers,
        ) {
            break 'outer;
        }

        crossbeam_channel::select! {
            recv(demand_rx) -> d => {
                match d {
                    Ok(DemandCommand::Request { receiver_id, count }) => {
                        queue_demand(
                            &mut head_demand,
                            &mut pending_demands,
                            &receivers,
                            receiver_id,
                            count,
                        );
                    }
                    Err(_) => demand_closed = true,
                }
            }
            recv(control_rx) -> cmd => {
                if handle_control(
                    cmd,
                    &mut receivers,
                    &mut recovered,
                    &mut head_demand,
                    &mut pending_demands,
                    &mut graceful_shutdown,
                    &mut cancelled,
                ) {
                    break 'outer;
                }
            }
            default(std::time::Duration::from_millis(1)) => {}
        }
    }

    if cancelled {
        shared.status.scheduler_exited();
        return;
    }

    shared
        .lifecycle
        .store(NO_MORE_WORK, Ordering::SeqCst);
    shared.status.no_more_work();
    drop_receiver_senders(&mut receivers);
    pending_demands.clear();
    shared.status.scheduler_exited();
}

fn recover_receiver<T, S: StatusSink>(
    receiver_id: ReceiverId,
    data_rx: Receiver<T>,
    receivers: &mut HashMap<ReceiverId, ReceiverState<T>>,
    recovered: &mut VecDeque<T>,
    status: &S,
) {
    let mut count = 0usize;
    if let Some(state) = receivers.remove(&receiver_id) {
        state.in_flight.store(0, Ordering::SeqCst);
        for item in data_rx.try_iter() {
            recovered.push_back(item);
            count += 1;
        }
    } else {
        for item in data_rx.try_iter() {
            recovered.push_back(item);
            count += 1;
        }
    }
    status.receiver_dropped(receiver_id, count);
}

fn all_in_flight_zero<T>(receivers: &HashMap<ReceiverId, ReceiverState<T>>) -> bool {
    receivers
        .values()
        .filter(|s| s.active)
        .all(|s| s.in_flight.load(Ordering::SeqCst) == 0)
}

fn try_finish_no_more_work<T: Send + 'static, S: StatusSink>(
    shared: &Shared<T, S>,
    graceful_shutdown: bool,
    ingress_closed: bool,
    recovered: &VecDeque<T>,
    no_head_demand: bool,
    receivers: &HashMap<ReceiverId, ReceiverState<T>>,
) -> bool {
    if !graceful_shutdown && lifecycle(shared) != SHUTTING_DOWN {
        return false;
    }
    if lifecycle(shared) == CANCELLED {
        return false;
    }
    if !ingress_closed || !recovered.is_empty() || !no_head_demand {
        return false;
    }
    if !all_in_flight_zero(receivers) {
        return false;
    }
    true
}

impl<T: Send + 'static> Feeder<T, NoStatus> {
    pub fn builder() -> FeederBuilder<T> {
        FeederBuilder {
            _marker: PhantomData,
        }
    }
}

impl<T: Send + 'static, S: StatusSink> Feeder<T, S> {
    pub fn id(&self) -> FeederId {
        self.id
    }

    pub fn tx(&self) -> Result<Sender<T>, AccessError> {
        let state = lifecycle(&self.shared);
        if state == CANCELLED {
            return Err(AccessError::Cancelled);
        }
        if state == NO_MORE_WORK {
            return Err(AccessError::NoMoreWork);
        }
        let guard = self.shared.ingress.lock().unwrap();
        match guard.as_ref() {
            Some(tx) => Ok(tx.clone()),
            None => {
                if state == SHUTTING_DOWN {
                    Err(AccessError::IngressSealed)
                } else if state == RUNNING {
                    Err(AccessError::IngressSealed)
                } else {
                    Err(AccessError::SchedulerStopped)
                }
            }
        }
    }

    pub fn rx(&self, prefetch: NonZeroUsize) -> Result<FeederRx<T, S>, AccessError> {
        let state = lifecycle(&self.shared);
        match state {
            CANCELLED => return Err(AccessError::Cancelled),
            NO_MORE_WORK => return Err(AccessError::NoMoreWork),
            RUNNING | SHUTTING_DOWN => {}
            _ => return Err(AccessError::SchedulerStopped),
        }

        let receiver_id = alloc_receiver_id(&self.shared);
        let (data_tx, data_rx) = crossbeam_channel::unbounded();
        let in_flight = Arc::new(AtomicUsize::new(0));

        self.shared
            .control_tx
            .send(ControlCommand::RegisterReceiver {
                receiver_id,
                data_tx,
                in_flight: Arc::clone(&in_flight),
                initial_prefetch: prefetch.get(),
            })
            .map_err(|_| AccessError::SchedulerStopped)?;

        self.shared
            .demand_tx
            .send(DemandCommand::Request {
                receiver_id,
                count: prefetch.get(),
            })
            .map_err(|_| AccessError::SchedulerStopped)?;

        self.shared.status.receiver_registered(receiver_id);

        Ok(FeederRx {
            receiver_id,
            data_rx: Mutex::new(Some(data_rx)),
            demand_tx: self.shared.demand_tx.clone(),
            in_flight,
            shared: Arc::clone(&self.shared),
            lagging: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
        })
    }

    pub fn graceful_shutdown(&self) {
        if self
            .shared
            .lifecycle
            .compare_exchange(RUNNING, SHUTTING_DOWN, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            *self.shared.ingress.lock().unwrap() = None;
            self.shared.status.graceful_shutdown_started();
            let _ = self.shared.control_tx.send(ControlCommand::GracefulShutdown);
        }
    }

    pub fn cancel(&self) {
        let prev = self
            .shared
            .lifecycle
            .swap(CANCELLED, Ordering::SeqCst);
        if prev != CANCELLED {
            let _ = self.shared.control_tx.send(ControlCommand::Cancel);
        }
    }
}

impl<T: Send + 'static, S: StatusSink> Clone for Feeder<T, S> {
    fn clone(&self) -> Self {
        Feeder {
            id: alloc_feeder_id(&self.shared),
            shared: Arc::clone(&self.shared),
        }
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

        let consumed = items.len();
        self.in_flight.fetch_sub(consumed, Ordering::SeqCst);
        self.send_refill(consumed);
        self.update_lag(max_usize, consumed);

        Ok(items)
    }

    pub fn get_one(&self) -> Result<T, GetError> {
        self.get(NonZeroUsize::new(1).unwrap())
            .map(|mut v| v.pop().unwrap())
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

        let consumed = items.len();
        self.in_flight.fetch_sub(consumed, Ordering::SeqCst);
        self.send_refill(consumed);
        self.update_lag(max_usize, consumed);

        TryGet::Items(items)
    }

    pub fn try_get_one(&self) -> TryGetOne<T> {
        match self.try_get(NonZeroUsize::new(1).unwrap()) {
            TryGet::Items(mut v) => TryGetOne::Item(v.pop().unwrap()),
            TryGet::Empty => TryGetOne::Empty,
            TryGet::InShutdown => TryGetOne::InShutdown,
            TryGet::NoMoreWork => TryGetOne::NoMoreWork,
            TryGet::Cancelled => TryGetOne::Cancelled,
        }
    }

    pub fn cancel(&self) {
        self.notify_drop();
    }

    fn send_refill(&self, count: usize) {
        if count == 0 {
            return;
        }
        let _ = self.demand_tx.send(DemandCommand::Request {
            receiver_id: self.receiver_id,
            count,
        });
    }

    fn disconnected_error(&self) -> Result<Vec<T>, GetError> {
        if lifecycle(&self.shared) == CANCELLED {
            Err(GetError::Cancelled)
        } else {
            Err(GetError::NoMoreWork)
        }
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

    fn update_lag(&self, requested: usize, received: usize) {
        let state = lifecycle(&self.shared);
        if state != RUNNING {
            return;
        }
        if received < requested {
            if !self.lagging.swap(true, Ordering::SeqCst) {
                self.shared.status.lagging(self.receiver_id, requested, received);
            }
        } else if self.lagging.swap(false, Ordering::SeqCst) {
            self.shared.status.recovered(self.receiver_id);
        }
    }

    fn notify_drop(&self) {
        if self.dropped.swap(true, Ordering::SeqCst) {
            return;
        }
        let data_rx = self.data_rx.lock().unwrap().take();
        if let Some(data_rx) = data_rx {
            let _ = self.shared.control_tx.send(ControlCommand::ReceiverDropped {
                receiver_id: self.receiver_id,
                data_rx,
            });
        }
    }
}

impl<T: Send + 'static, S: StatusSink> Drop for FeederRx<T, S> {
    fn drop(&mut self) {
        self.notify_drop();
    }
}

impl<T: Send + 'static, S: StatusSink> Drop for Shared<T, S> {
    fn drop(&mut self) {
        if let Some(handle) = self.scheduler.lock().unwrap().take() {
            let _ = handle.join();
        }
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

    #[test]
    fn tx_returns_sender_while_running() {
        let feeder = make_feeder::<i32>();
        assert!(feeder.tx().is_ok());
    }

    #[test]
    fn tx_returns_ingress_sealed_after_graceful_shutdown() {
        let feeder = make_feeder::<i32>();
        feeder.graceful_shutdown();
        assert!(matches!(feeder.tx(), Err(AccessError::IngressSealed)));
    }

    #[test]
    fn rx_allowed_during_graceful_shutdown() {
        let feeder = make_feeder::<i32>();
        feeder.graceful_shutdown();
        assert!(feeder.rx(NonZeroUsize::new(1).unwrap()).is_ok());
    }

    #[test]
    fn rx_rejected_after_cancel() {
        let feeder = make_feeder::<i32>();
        feeder.cancel();
        std::thread::sleep(Duration::from_millis(50));
        assert!(matches!(
            feeder.rx(NonZeroUsize::new(1).unwrap()),
            Err(AccessError::Cancelled)
        ));
    }

    #[test]
    fn get_returns_non_empty_vec_up_to_max() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
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
        let rx = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(42).unwrap();
        assert_eq!(rx.get_one().unwrap(), 42);
    }

    #[test]
    fn try_get_empty_while_running() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
        assert!(matches!(rx.try_get(NonZeroUsize::new(3).unwrap()), TryGet::Empty));
    }

    #[test]
    fn try_get_in_shutdown_when_empty_and_shutdown_started() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
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
        let rx = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
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
        let rx = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
        let feeder2 = feeder.clone();
        let handle = std::thread::spawn(move || rx.get_one());
        std::thread::sleep(Duration::from_millis(50));
        feeder2.cancel();
        assert_eq!(handle.join().unwrap(), Err(GetError::Cancelled));
    }

    #[test]
    fn receiver_refills_only_consumed_count() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(NonZeroUsize::new(2).unwrap()).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        let batch = rx.get(NonZeroUsize::new(2).unwrap()).unwrap();
        assert_eq!(batch.len(), 2);
        tx.send(4).unwrap();
        let one = rx.get_one().unwrap();
        assert_eq!(one, 3);
    }

    #[test]
    fn recovered_work_is_prioritized_before_ingress() {
        let feeder = make_feeder::<i32>();
        let rx1 = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
        let tx = feeder.tx().unwrap();
        tx.send(100).unwrap();
        let _ = rx1.get_one().unwrap();
        tx.send(200).unwrap();
        drop(rx1);
        std::thread::sleep(Duration::from_millis(50));
        let rx2 = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
        tx.send(300).unwrap();
        let first = rx2.get_one().unwrap();
        assert_eq!(first, 200);
    }

    #[test]
    fn receiver_drop_recovers_buffered_items_without_loss() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(NonZeroUsize::new(5).unwrap()).unwrap();
        let tx = feeder.tx().unwrap();
        for i in 0..10 {
            tx.send(i).unwrap();
        }
        std::thread::sleep(Duration::from_millis(100));
        drop(rx);
        std::thread::sleep(Duration::from_millis(100));
        let rx2 = feeder.rx(NonZeroUsize::new(10).unwrap()).unwrap();
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
    fn no_more_work_waits_for_in_flight_items() {
        let feeder = make_feeder::<i32>();
        let rx = feeder.rx(NonZeroUsize::new(3).unwrap()).unwrap();
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
        assert!(feeder.rx(NonZeroUsize::new(1).unwrap()).is_ok());
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
            feeder.rx(NonZeroUsize::new(1).unwrap()),
            Err(AccessError::NoMoreWork)
        ));
    }

    #[test]
    fn status_emits_lag_once_then_recovered_once() {
        let (feeder, status_rx) = Feeder::<i32>::builder().build_with_status();
        let rx = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
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
        let rx = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
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
        assert!(events.iter().any(|e| matches!(e, StatusEvent::ReceiverRegistered { .. })));
        assert!(events.iter().any(|e| matches!(e, StatusEvent::GracefulShutdownStarted)));
        assert!(events.iter().any(|e| matches!(e, StatusEvent::NoMoreWork)));
        assert!(events.iter().any(|e| matches!(e, StatusEvent::SchedulerExited)));
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
        let rx1 = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
        let rx2 = feeder.rx(NonZeroUsize::new(1).unwrap()).unwrap();
        assert!(rx2.id().0 > rx1.id().0);
    }
}
