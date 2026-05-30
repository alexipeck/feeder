use super::{checksum, make_payload, payload_seed, payloads_per_producer};
use criterion::black_box;
use feeder::Feeder;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

pub const CONSUMER0_BATCH: usize = 3;
pub const FEEDER_CONSUMER0_LOW: usize = 4;
pub const FEEDER_CONSUMER0_HIGH: usize = 9;
pub const FEEDER_OTHER_LOW: usize = 1;
pub const FEEDER_OTHER_HIGH: usize = 3;
pub const TUNED_CONSUMER0_LOW: usize = 16;
pub const TUNED_CONSUMER0_HIGH: usize = 64;
pub const TUNED_OTHER_LOW: usize = 8;
pub const TUNED_OTHER_HIGH: usize = 32;
pub const FEEDER_SEND_BATCH: usize = 32;
pub const DRAIN_CONSUMER_BATCH: usize = 8;

#[derive(Clone, Copy)]
pub enum ProducerMode {
    SingleSend,
    BatchSend(usize),
}

#[derive(Clone, Copy)]
pub enum ConsumerMode {
    GetOne,
    DrainRest(usize),
}

#[derive(Clone, Copy)]
pub struct WaterMarks {
    pub consumer0_low: usize,
    pub consumer0_high: usize,
    pub other_low: usize,
    pub other_high: usize,
}

pub const DEFAULT_WATER: WaterMarks = WaterMarks {
    consumer0_low: FEEDER_CONSUMER0_LOW,
    consumer0_high: FEEDER_CONSUMER0_HIGH,
    other_low: FEEDER_OTHER_LOW,
    other_high: FEEDER_OTHER_HIGH,
};

pub const TUNED_WATER: WaterMarks = WaterMarks {
    consumer0_low: TUNED_CONSUMER0_LOW,
    consumer0_high: TUNED_CONSUMER0_HIGH,
    other_low: TUNED_OTHER_LOW,
    other_high: TUNED_OTHER_HIGH,
};

#[derive(Clone, Copy)]
pub struct FeederBenchConfig {
    pub suffix: &'static str,
    pub water: WaterMarks,
    pub producer_mode: ProducerMode,
    pub consumer_mode: ConsumerMode,
}

pub const BENEFIT_MATRIX: [FeederBenchConfig; 8] = [
    FeederBenchConfig {
        suffix: "baseline",
        water: DEFAULT_WATER,
        producer_mode: ProducerMode::SingleSend,
        consumer_mode: ConsumerMode::GetOne,
    },
    FeederBenchConfig {
        suffix: "tuned_water",
        water: TUNED_WATER,
        producer_mode: ProducerMode::SingleSend,
        consumer_mode: ConsumerMode::GetOne,
    },
    FeederBenchConfig {
        suffix: "tx_batch",
        water: DEFAULT_WATER,
        producer_mode: ProducerMode::BatchSend(FEEDER_SEND_BATCH),
        consumer_mode: ConsumerMode::GetOne,
    },
    FeederBenchConfig {
        suffix: "consumer_drain",
        water: DEFAULT_WATER,
        producer_mode: ProducerMode::SingleSend,
        consumer_mode: ConsumerMode::DrainRest(DRAIN_CONSUMER_BATCH),
    },
    FeederBenchConfig {
        suffix: "water_tx",
        water: TUNED_WATER,
        producer_mode: ProducerMode::BatchSend(FEEDER_SEND_BATCH),
        consumer_mode: ConsumerMode::GetOne,
    },
    FeederBenchConfig {
        suffix: "water_drain",
        water: TUNED_WATER,
        producer_mode: ProducerMode::SingleSend,
        consumer_mode: ConsumerMode::DrainRest(DRAIN_CONSUMER_BATCH),
    },
    FeederBenchConfig {
        suffix: "tx_drain",
        water: DEFAULT_WATER,
        producer_mode: ProducerMode::BatchSend(FEEDER_SEND_BATCH),
        consumer_mode: ConsumerMode::DrainRest(DRAIN_CONSUMER_BATCH),
    },
    FeederBenchConfig {
        suffix: "combined",
        water: TUNED_WATER,
        producer_mode: ProducerMode::BatchSend(FEEDER_SEND_BATCH),
        consumer_mode: ConsumerMode::DrainRest(DRAIN_CONSUMER_BATCH),
    },
];

pub struct RunTotals {
    pub received: u64,
    pub received_checksum: u64,
}

#[cfg_attr(not(feature = "perf-stats"), allow(dead_code))]
pub fn feeder_stats_enabled() -> bool {
    std::env::var("FEEDER_STATS").is_ok_and(|v| v == "1")
}

pub fn spawn_feeder_consumers(
    receivers: Vec<feeder::FeederRx<Vec<u64>>>,
    with_checksum: bool,
    consumer_mode: ConsumerMode,
    received_count: Arc<AtomicU64>,
    received_checksum: Arc<AtomicU64>,
) -> Vec<thread::JoinHandle<()>> {
    let get_max = NonZeroUsize::new(CONSUMER0_BATCH).unwrap();
    receivers
        .into_iter()
        .enumerate()
        .map(|(i, rx)| {
            let received_count = Arc::clone(&received_count);
            let received_checksum = Arc::clone(&received_checksum);
            thread::spawn(move || {
                let mut local_count = 0u64;
                let mut local_checksum = 0u64;
                if i == 0 {
                    loop {
                        match rx.get(get_max) {
                            Ok(batch) => {
                                for payload in batch {
                                    if with_checksum {
                                        local_checksum =
                                            local_checksum.wrapping_add(checksum(&payload));
                                    } else {
                                        black_box(&payload);
                                    }
                                    local_count += 1;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                } else {
                    loop {
                        match consumer_mode {
                            ConsumerMode::GetOne => match rx.get_one() {
                                Ok(payload) => {
                                    if with_checksum {
                                        local_checksum =
                                            local_checksum.wrapping_add(checksum(&payload));
                                    } else {
                                        black_box(payload);
                                    }
                                    local_count += 1;
                                }
                                Err(_) => break,
                            },
                            ConsumerMode::DrainRest(max) => {
                                match rx.get(NonZeroUsize::new(max).unwrap()) {
                                    Ok(batch) => {
                                        for payload in batch {
                                            if with_checksum {
                                                local_checksum =
                                                    local_checksum.wrapping_add(checksum(&payload));
                                            } else {
                                                black_box(&payload);
                                            }
                                            local_count += 1;
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        }
                    }
                }
                received_count.fetch_add(local_count, Ordering::Relaxed);
                received_checksum.fetch_add(local_checksum, Ordering::Relaxed);
            })
        })
        .collect()
}

pub fn run_feeder_bench(
    config: FeederBenchConfig,
    scheduler_threads: usize,
    producers: usize,
    consumer_threads: usize,
    with_checksum: bool,
) -> (RunTotals, Feeder<Vec<u64>>) {
    let wall_start = Instant::now();
    let feeder = Feeder::<Vec<u64>>::builder()
        .scheduler_threads(NonZeroUsize::new(scheduler_threads).unwrap())
        .build();
    #[cfg(feature = "perf-stats")]
    feeder.reset_stats();

    let water = config.water;
    let water0 = (
        NonZeroUsize::new(water.consumer0_low).unwrap(),
        NonZeroUsize::new(water.consumer0_high).unwrap(),
    );
    let water_rest = (
        NonZeroUsize::new(water.other_low).unwrap(),
        NonZeroUsize::new(water.other_high).unwrap(),
    );
    let per_producer = payloads_per_producer(producers);

    let mut receivers = Vec::with_capacity(consumer_threads);
    for i in 0..consumer_threads {
        let (low, high) = if i == 0 { water0 } else { water_rest };
        receivers.push(feeder.rx(low, high).expect("rx"));
    }

    let tx = feeder.tx().expect("tx");
    let received_count = Arc::new(AtomicU64::new(0));
    let received_checksum = Arc::new(AtomicU64::new(0));

    let consumer_handles = spawn_feeder_consumers(
        receivers,
        with_checksum,
        config.consumer_mode,
        Arc::clone(&received_count),
        Arc::clone(&received_checksum),
    );

    let producer_mode = config.producer_mode;
    let producer_handles: Vec<_> = (0..producers)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || match producer_mode {
                ProducerMode::SingleSend => {
                    for i in 0..per_producer {
                        let payload = make_payload(payload_seed(p, i as u64));
                        tx.send(payload).unwrap();
                    }
                }
                ProducerMode::BatchSend(batch_size) => {
                    let mut i = 0usize;
                    while i < per_producer {
                        let batch_len = batch_size.min(per_producer - i);
                        let mut batch = Vec::with_capacity(batch_len);
                        for offset in 0..batch_len {
                            batch.push(make_payload(payload_seed(p, (i + offset) as u64)));
                        }
                        tx.send_batch(batch).unwrap();
                        i += batch_len;
                    }
                }
            })
        })
        .collect();

    for h in producer_handles {
        h.join().unwrap();
    }
    feeder.graceful_shutdown();
    drop(tx);

    for h in consumer_handles {
        h.join().unwrap();
    }

    let totals = RunTotals {
        received: received_count.load(Ordering::Relaxed),
        received_checksum: received_checksum.load(Ordering::Relaxed),
    };
    eprintln!(
        "  wall {:.2}s, {} payloads",
        wall_start.elapsed().as_secs_f64(),
        totals.received
    );
    (totals, feeder)
}

pub fn config_description(config: FeederBenchConfig) -> String {
    let water = config.water;
    let producer = match config.producer_mode {
        ProducerMode::SingleSend => "send".to_string(),
        ProducerMode::BatchSend(n) => format!("send_batch({n})"),
    };
    let consumer = match config.consumer_mode {
        ConsumerMode::GetOne => "get_one".to_string(),
        ConsumerMode::DrainRest(n) => format!("get(max={n})"),
    };
    format!(
        "water c0={}:{} other={}:{} producer={producer} rest={consumer}",
        water.consumer0_low, water.consumer0_high, water.other_low, water.other_high
    )
}
