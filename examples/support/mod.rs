use feeder::Feeder;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

pub const PAYLOAD_LEN: usize = 64;
pub const WORK_TOTAL: usize = 14_500_000;
pub const PRODUCERS: usize = 10;
pub const SCHEDULER_THREADS: usize = 4;
pub const CONSUMER0_BATCH: usize = 3;
pub const WATER_LOW: usize = 32;
pub const WATER_HIGH: usize = 64;
pub const SEND_BATCH: usize = 32;
pub const DRAIN_CONSUMER_BATCH: usize = 8;

#[derive(Clone, Copy)]
pub enum ConsumerMode {
    GetOne,
    DrainRest(usize),
}

fn consumer_threads() -> usize {
    thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1)
}

fn payloads_per_producer(producers: usize) -> usize {
    WORK_TOTAL / producers
}

fn make_payload(seed: u64) -> Vec<u64> {
    (0..PAYLOAD_LEN as u64)
        .map(|i| seed.wrapping_mul(31).wrapping_add(i))
        .collect()
}

fn checksum(payload: &[u64]) -> u64 {
    payload.iter().fold(0u64, |a, &b| a.wrapping_add(b))
}

fn payload_seed(producer: usize, seq: u64) -> u64 {
    (producer as u64) << 32 | seq
}

fn expected_totals(producers: usize) -> (usize, u64) {
    let per = payloads_per_producer(producers);
    let mut count = 0usize;
    let mut cs = 0u64;
    for p in 0..producers {
        for i in 0..per {
            let payload = make_payload(payload_seed(p, i as u64));
            cs = cs.wrapping_add(checksum(&payload));
            count += 1;
        }
    }
    (count, cs)
}

fn spawn_consumers(
    receivers: Vec<feeder::FeederRx<Vec<u64>>>,
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
                                    local_checksum =
                                        local_checksum.wrapping_add(checksum(&payload));
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
                                    local_checksum =
                                        local_checksum.wrapping_add(checksum(&payload));
                                    local_count += 1;
                                }
                                Err(_) => break,
                            },
                            ConsumerMode::DrainRest(max) => {
                                match rx.get(NonZeroUsize::new(max).unwrap()) {
                                    Ok(batch) => {
                                        for payload in batch {
                                            local_checksum =
                                                local_checksum.wrapping_add(checksum(&payload));
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

pub fn run_combined_uniform(consumer_mode: ConsumerMode) {
    let producers = PRODUCERS;
    let consumers = consumer_threads();
    let (expected_count, expected_checksum) = expected_totals(producers);
    let wall_start = Instant::now();

    let feeder = Feeder::<Vec<u64>>::builder()
        .scheduler_threads(NonZeroUsize::new(SCHEDULER_THREADS).unwrap())
        .build();

    let water_low = NonZeroUsize::new(WATER_LOW).unwrap();
    let water_high = NonZeroUsize::new(WATER_HIGH).unwrap();
    let per_producer = payloads_per_producer(producers);

    let mut receivers = Vec::with_capacity(consumers);
    for _ in 0..consumers {
        receivers.push(feeder.rx(water_low, water_high).expect("rx"));
    }

    let tx = feeder.tx().expect("tx");
    let received_count = Arc::new(AtomicU64::new(0));
    let received_checksum = Arc::new(AtomicU64::new(0));

    let consumer_handles = spawn_consumers(
        receivers,
        consumer_mode,
        Arc::clone(&received_count),
        Arc::clone(&received_checksum),
    );

    let producer_handles: Vec<_> = (0..producers)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                let mut i = 0usize;
                while i < per_producer {
                    let batch_len = SEND_BATCH.min(per_producer - i);
                    let mut batch = Vec::with_capacity(batch_len);
                    for offset in 0..batch_len {
                        batch.push(make_payload(payload_seed(p, (i + offset) as u64)));
                    }
                    tx.send_batch(batch).unwrap();
                    i += batch_len;
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

    let received = received_count.load(Ordering::Relaxed);
    let received_checksum = received_checksum.load(Ordering::Relaxed);
    assert_eq!(received as usize, expected_count);
    assert_eq!(received_checksum, expected_checksum);
    black_box(received);

    let wall = wall_start.elapsed().as_secs_f64();
    let thrpt = received as f64 / wall / 1_000_000.0;
    let rest = match consumer_mode {
        ConsumerMode::GetOne => "get_one",
        ConsumerMode::DrainRest(n) => {
            if n == DRAIN_CONSUMER_BATCH {
                "get(max=8)"
            } else {
                "get(max=?)"
            }
        }
    };
    println!(
        "combined_uniform {rest}: {producers} producers, {consumers} consumers, water {WATER_LOW}:{WATER_HIGH}, c0 get({CONSUMER0_BATCH}), send_batch({SEND_BATCH})"
    );
    println!(
        "  wall {wall:.2}s, {received} payloads, {thrpt:.2} Melem/s"
    );
}
