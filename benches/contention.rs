mod common;

use common::{checksum, expected_totals, make_payload, payload_seed, payloads_per_producer, work_total};
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode,
};
use feeder::Feeder;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const PAYLOAD_LEN: usize = common::PAYLOAD_LEN;
const CONSUMER_THREADS: usize = 32;
const PRODUCER_COUNTS: [usize; 3] = [4, 8, 10];
const CONSUMER0_BATCH: usize = 3;
const FEEDER_CONSUMER0_LOW: usize = 4;
const FEEDER_CONSUMER0_HIGH: usize = 9;
const FEEDER_OTHER_LOW: usize = 1;
const FEEDER_OTHER_HIGH: usize = 3;
const CRITERION_SAMPLES: usize = 10;
const TUNING_PRODUCERS: usize = 10;
const TARGET_WALL_SECS: f64 = 10.0;
const MEASUREMENT_MARGIN: f64 = 1.2;

fn measurement_budget() -> Duration {
    if std::env::args().any(|a| a == "--test") {
        Duration::from_secs(5)
    } else {
        Duration::from_secs_f64(
            TARGET_WALL_SECS * MEASUREMENT_MARGIN * CRITERION_SAMPLES as f64,
        )
    }
}

fn log_benchmark_topology() {
    eprintln!();
    eprintln!("contention benchmark topology");
    eprintln!("  fixed total payloads per run: {} (tuned for ~{TARGET_WALL_SECS:.0}s wall @ {TUNING_PRODUCERS} producers, {CONSUMER_THREADS} consumers, feeder)", work_total());
    eprintln!("  per-producer payloads: total / producer count for that case");
    eprintln!(
        "  criterion: flat sampling, {CRITERION_SAMPLES} iterations (= samples), ~{:.0}s measurement budget",
        measurement_budget().as_secs_f64()
    );
    eprintln!("  producer counts per case: {:?}", PRODUCER_COUNTS);
    eprintln!("  consumer threads (every case): {CONSUMER_THREADS}");
    eprintln!("  consumer 0 (1 thread):");
    eprintln!("    feeder:    low={FEEDER_CONSUMER0_LOW},high={FEEDER_CONSUMER0_HIGH}, get(max={CONSUMER0_BATCH})");
    eprintln!("    crossbeam: {CONSUMER0_BATCH} recv() calls per loop iteration");
    eprintln!("    kanal:     {CONSUMER0_BATCH} recv() calls per loop iteration");
    eprintln!(
        "  consumers 1..{} ({} threads):",
        CONSUMER_THREADS - 1,
        CONSUMER_THREADS - 1
    );
    eprintln!("    feeder:    low={FEEDER_OTHER_LOW},high={FEEDER_OTHER_HIGH}, get_one() per loop");
    eprintln!("    crossbeam: 1 recv() call per loop iteration");
    eprintln!("    kanal:     1 recv() call per loop iteration");
    eprintln!("  payload type: Vec<u64> len {PAYLOAD_LEN}");
    eprintln!();
}

#[cfg(feature = "perf-stats")]
fn feeder_stats_enabled() -> bool {
    std::env::var("FEEDER_STATS").is_ok_and(|v| v == "1")
}

fn benchmark_group_name(backend: &str, producers: usize) -> String {
    format!("{backend}/producers={producers}/consumers={CONSUMER_THREADS}")
}

fn benchmark_id(backend: &str) -> BenchmarkId {
    let consumer0 = match backend {
        "feeder" => format!("c0:low={FEEDER_CONSUMER0_LOW},high={FEEDER_CONSUMER0_HIGH},get({CONSUMER0_BATCH})"),
        "crossbeam" | "kanal" => format!("c0:recv_x{CONSUMER0_BATCH}"),
        _ => "c0:?".to_string(),
    };
    let consumers_rest = match backend {
        "feeder" => format!("c1-{}:low={FEEDER_OTHER_LOW},high={FEEDER_OTHER_HIGH},get_one", CONSUMER_THREADS - 1),
        "crossbeam" | "kanal" => format!("c1-{}:recv_x1", CONSUMER_THREADS - 1),
        _ => format!("c1-{}:?", CONSUMER_THREADS - 1),
    };
    BenchmarkId::new(consumer0, consumers_rest)
}

struct RunTotals {
    received: u64,
    received_checksum: u64,
}

fn run_feeder(producers: usize) -> RunTotals {
    let wall_start = Instant::now();
    let feeder = Feeder::<Vec<u64>>::builder().build();
    let water0 = (
        NonZeroUsize::new(FEEDER_CONSUMER0_LOW).unwrap(),
        NonZeroUsize::new(FEEDER_CONSUMER0_HIGH).unwrap(),
    );
    let water_rest = (
        NonZeroUsize::new(FEEDER_OTHER_LOW).unwrap(),
        NonZeroUsize::new(FEEDER_OTHER_HIGH).unwrap(),
    );
    let get_max = NonZeroUsize::new(CONSUMER0_BATCH).unwrap();
    let per_producer = payloads_per_producer(producers);

    let mut receivers = Vec::with_capacity(CONSUMER_THREADS);
    for i in 0..CONSUMER_THREADS {
        let (low, high) = if i == 0 { water0 } else { water_rest };
        receivers.push(feeder.rx(low, high).expect("rx"));
    }

    let tx = feeder.tx().expect("tx");
    let received_count = Arc::new(AtomicU64::new(0));
    let received_checksum = Arc::new(AtomicU64::new(0));

    let consumer_handles: Vec<_> = receivers
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
                                    local_checksum = local_checksum.wrapping_add(checksum(&payload));
                                    local_count += 1;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                } else {
                    loop {
                        match rx.get_one() {
                            Ok(payload) => {
                                local_checksum = local_checksum.wrapping_add(checksum(&payload));
                                local_count += 1;
                            }
                            Err(_) => break,
                        }
                    }
                }
                received_count.fetch_add(local_count, Ordering::Relaxed);
                received_checksum.fetch_add(local_checksum, Ordering::Relaxed);
            })
        })
        .collect();

    let producer_handles: Vec<_> = (0..producers)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..per_producer {
                    let payload = make_payload(payload_seed(p, i as u64));
                    tx.send(payload).unwrap();
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

    #[cfg(feature = "perf-stats")]
    if feeder_stats_enabled() {
        feeder
            .stats_snapshot()
            .print_stderr(&format!("contention/p{producers}"));
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
    totals
}

fn run_crossbeam(producers: usize) -> RunTotals {
    let wall_start = Instant::now();
    let (tx, rx) = crossbeam_channel::unbounded::<Vec<u64>>();
    let per_producer = payloads_per_producer(producers);
    let received_count = Arc::new(AtomicU64::new(0));
    let received_checksum = Arc::new(AtomicU64::new(0));

    let consumer_handles: Vec<_> = (0..CONSUMER_THREADS)
        .map(|i| {
            let rx = rx.clone();
            let received_count = Arc::clone(&received_count);
            let received_checksum = Arc::clone(&received_checksum);
            thread::spawn(move || {
                let mut local_count = 0u64;
                let mut local_checksum = 0u64;
                loop {
                    if i == 0 {
                        let mut batch = 0u64;
                        for _ in 0..CONSUMER0_BATCH {
                            match rx.recv() {
                                Ok(payload) => {
                                    local_checksum =
                                        local_checksum.wrapping_add(checksum(&payload));
                                    batch += 1;
                                }
                                Err(_) => {
                                    local_count += batch;
                                    received_count.fetch_add(local_count, Ordering::Relaxed);
                                    received_checksum
                                        .fetch_add(local_checksum, Ordering::Relaxed);
                                    return;
                                }
                            }
                        }
                        local_count += batch;
                    } else {
                        match rx.recv() {
                            Ok(payload) => {
                                local_checksum = local_checksum.wrapping_add(checksum(&payload));
                                local_count += 1;
                            }
                            Err(_) => {
                                received_count.fetch_add(local_count, Ordering::Relaxed);
                                received_checksum
                                    .fetch_add(local_checksum, Ordering::Relaxed);
                                return;
                            }
                        }
                    }
                }
            })
        })
        .collect();

    let producer_handles: Vec<_> = (0..producers)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..per_producer {
                    let payload = make_payload(payload_seed(p, i as u64));
                    tx.send(payload).unwrap();
                }
            })
        })
        .collect();

    for h in producer_handles {
        h.join().unwrap();
    }
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
    totals
}

fn run_kanal(producers: usize) -> RunTotals {
    let wall_start = Instant::now();
    let (tx, rx) = kanal::unbounded::<Vec<u64>>();
    let per_producer = payloads_per_producer(producers);
    let received_count = Arc::new(AtomicU64::new(0));
    let received_checksum = Arc::new(AtomicU64::new(0));

    let consumer_handles: Vec<_> = (0..CONSUMER_THREADS)
        .map(|i| {
            let rx = rx.clone();
            let received_count = Arc::clone(&received_count);
            let received_checksum = Arc::clone(&received_checksum);
            thread::spawn(move || {
                let mut local_count = 0u64;
                let mut local_checksum = 0u64;
                loop {
                    if i == 0 {
                        let mut batch = 0u64;
                        for _ in 0..CONSUMER0_BATCH {
                            match rx.recv() {
                                Ok(payload) => {
                                    local_checksum =
                                        local_checksum.wrapping_add(checksum(&payload));
                                    batch += 1;
                                }
                                Err(_) => {
                                    local_count += batch;
                                    received_count.fetch_add(local_count, Ordering::Relaxed);
                                    received_checksum
                                        .fetch_add(local_checksum, Ordering::Relaxed);
                                    return;
                                }
                            }
                        }
                        local_count += batch;
                    } else {
                        match rx.recv() {
                            Ok(payload) => {
                                local_checksum = local_checksum.wrapping_add(checksum(&payload));
                                local_count += 1;
                            }
                            Err(_) => {
                                received_count.fetch_add(local_count, Ordering::Relaxed);
                                received_checksum
                                    .fetch_add(local_checksum, Ordering::Relaxed);
                                return;
                            }
                        }
                    }
                }
            })
        })
        .collect();

    let producer_handles: Vec<_> = (0..producers)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..per_producer {
                    let payload = make_payload(payload_seed(p, i as u64));
                    tx.send(payload).unwrap();
                }
            })
        })
        .collect();

    for h in producer_handles {
        h.join().unwrap();
    }
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
    totals
}

fn bench_impl(
    c: &mut Criterion,
    backend: &str,
    producers: usize,
    run: fn(usize) -> RunTotals,
) {
    let (expected_count, expected_checksum) = expected_totals(producers);
    eprintln!(
        "running {backend}: {producers} producer threads, {CONSUMER_THREADS} consumer threads, {expected_count} payloads"
    );

    let mut group = c.benchmark_group(benchmark_group_name(backend, producers));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(CRITERION_SAMPLES);
    group.measurement_time(measurement_budget());
    group.throughput(criterion::Throughput::Elements(expected_count as u64));
    group.bench_with_input(benchmark_id(backend), &producers, |b, &producers| {
        b.iter(|| {
            let totals = run(producers);
            assert_eq!(totals.received as usize, expected_count);
            assert_eq!(totals.received_checksum, expected_checksum);
            black_box(totals);
        });
    });
    group.finish();
}

fn tune_work_load() {
    eprintln!(
        "tuning: feeder, {TUNING_PRODUCERS} producers, {CONSUMER_THREADS} consumers, {} total payloads",
        work_total()
    );
    let start = Instant::now();
    let totals = run_feeder(TUNING_PRODUCERS);
    let wall = start.elapsed().as_secs_f64();
    eprintln!(
        "tuning result: wall {wall:.2}s (target {TARGET_WALL_SECS:.0}s), received {}",
        totals.received
    );
}

fn contention_benchmarks(c: &mut Criterion) {
    if std::env::var("TUNE_WORK").is_ok() && !std::env::args().any(|a| a == "--test") {
        tune_work_load();
        return;
    }
    log_benchmark_topology();
    for &producers in &PRODUCER_COUNTS {
        bench_impl(c, "feeder", producers, run_feeder);
        bench_impl(c, "crossbeam", producers, run_crossbeam);
        bench_impl(c, "kanal", producers, run_kanal);
    }
}

criterion_group!(benches, contention_benchmarks);
criterion_main!(benches);
