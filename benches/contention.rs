mod common;

use common::feeder_harness::{
    config_description, run_feeder_bench, BENEFIT_MATRIX, ConsumerMode,
    FeederBenchConfig, ProducerMode, CONSUMER0_BATCH, DRAIN_CONSUMER_BATCH,
};
use common::{
    checksum, expected_totals, make_payload, payload_seed, payloads_per_producer, work_total,
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const PAYLOAD_LEN: usize = common::PAYLOAD_LEN;
const SCHEDULER_THREADS: usize = 4;
const PRODUCER_COUNTS: [usize; 3] = [4, 8, 10];
const CRITERION_SAMPLES: usize = 10;
const TUNING_PRODUCERS: usize = 10;
const TARGET_WALL_SECS: f64 = 10.0;
const MEASUREMENT_MARGIN: f64 = 1.2;

fn benchmark_parallelism() -> usize {
    thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1)
}

fn measurement_budget() -> Duration {
    if std::env::args().any(|a| a == "--test") {
        Duration::from_secs(5)
    } else {
        Duration::from_secs_f64(TARGET_WALL_SECS * MEASUREMENT_MARGIN * CRITERION_SAMPLES as f64)
    }
}

fn log_benchmark_topology() {
    let consumer_threads = benchmark_parallelism();
    eprintln!();
    eprintln!("contention benchmark topology");
    eprintln!(
        "  fixed total payloads per run: {} (tuned for ~{TARGET_WALL_SECS:.0}s wall @ {TUNING_PRODUCERS} producers, max-parallelism consumers, feeder)",
        work_total()
    );
    eprintln!("  per-producer payloads: total / producer count for that case");
    eprintln!(
        "  criterion: flat sampling, {CRITERION_SAMPLES} iterations (= samples), ~{:.0}s measurement budget",
        measurement_budget().as_secs_f64()
    );
    eprintln!("  producer counts per case: {:?}", PRODUCER_COUNTS);
    eprintln!("  consumer threads (every case): {consumer_threads}");
    eprintln!("  consumer 0 (all feeder variants): get(max={CONSUMER0_BATCH})");
    eprintln!("  feeder matrix (8 configs x producer counts):");
    for config in BENEFIT_MATRIX {
        eprintln!("    feeder_{}: {}", config.suffix, config_description(config));
    }
    eprintln!("  crossbeam consumer 0: {CONSUMER0_BATCH} recv() per loop");
    eprintln!("  kanal consumer 0: {CONSUMER0_BATCH} recv() per loop");
    eprintln!(
        "  crossbeam/kanal consumers 1..{}: 1 recv() per loop",
        consumer_threads - 1
    );
    eprintln!(
        "  crossbeam_drain consumers 1..{}: recv + try_recv up to {DRAIN_CONSUMER_BATCH}",
        consumer_threads - 1
    );
    eprintln!("  set FEEDER_STATS=1 for scheduler counters on feeder runs");
    eprintln!("  payload type: Vec<u64> len {PAYLOAD_LEN}");
    eprintln!();
}

fn benchmark_group_name(backend: &str, producers: usize) -> String {
    format!(
        "{backend}/producers={producers}/consumers={}",
        benchmark_parallelism()
    )
}

fn benchmark_id_for_feeder(config: FeederBenchConfig) -> BenchmarkId {
    let consumer_threads = benchmark_parallelism();
    let water = config.water;
    let consumer0 = format!(
        "c0:low={},high={},get({CONSUMER0_BATCH})",
        water.consumer0_low, water.consumer0_high
    );
    let rest = match config.consumer_mode {
        ConsumerMode::GetOne => {
            format!(
                "c1-{}:low={},high={},get_one",
                consumer_threads - 1,
                water.other_low,
                water.other_high
            )
        }
        ConsumerMode::DrainRest(max) => format!(
            "c1-{}:low={},high={},get({max})",
            consumer_threads - 1,
            water.other_low,
            water.other_high
        ),
    };
    let producer = match config.producer_mode {
        ProducerMode::SingleSend => "send",
        ProducerMode::BatchSend(n) => return BenchmarkId::new(
            format!("{consumer0}/producer=send_batch({n})"),
            rest,
        ),
    };
    BenchmarkId::new(format!("{consumer0}/producer={producer}"), rest)
}

fn benchmark_id_channel(backend: &str) -> BenchmarkId {
    let consumer_threads = benchmark_parallelism();
    let consumer0 = format!("c0:recv_x{CONSUMER0_BATCH}");
    let consumers_rest = match backend {
        "crossbeam_drain" => format!(
            "c1-{}:recv_then_try_recv_to_{DRAIN_CONSUMER_BATCH}",
            consumer_threads - 1
        ),
        "crossbeam" | "kanal" => format!("c1-{}:recv_x1", consumer_threads - 1),
        _ => format!("c1-{}:?", consumer_threads - 1),
    };
    BenchmarkId::new(consumer0, consumers_rest)
}

struct RunTotals {
    received: u64,
    received_checksum: u64,
}

fn run_feeder_config(producers: usize, config: FeederBenchConfig) -> RunTotals {
    let consumer_threads = benchmark_parallelism();
    let (totals, feeder) = run_feeder_bench(
        config,
        SCHEDULER_THREADS,
        producers,
        consumer_threads,
        true,
    );
    #[cfg(feature = "perf-stats")]
    if common::feeder_harness::feeder_stats_enabled() {
        feeder.stats_snapshot().print_stderr(&format!(
            "contention_{}/p{producers}",
            config.suffix
        ));
    }
    let _ = feeder;
    RunTotals {
        received: totals.received,
        received_checksum: totals.received_checksum,
    }
}

fn run_crossbeam(producers: usize) -> RunTotals {
    let wall_start = Instant::now();
    let consumer_threads = benchmark_parallelism();
    let (tx, rx) = crossbeam_channel::unbounded::<Vec<u64>>();
    let per_producer = payloads_per_producer(producers);
    let received_count = Arc::new(AtomicU64::new(0));
    let received_checksum = Arc::new(AtomicU64::new(0));

    let consumer_handles: Vec<_> = (0..consumer_threads)
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
                                    received_checksum.fetch_add(local_checksum, Ordering::Relaxed);
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
                                received_checksum.fetch_add(local_checksum, Ordering::Relaxed);
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

fn run_crossbeam_drain(producers: usize) -> RunTotals {
    let wall_start = Instant::now();
    let consumer_threads = benchmark_parallelism();
    let (tx, rx) = crossbeam_channel::unbounded::<Vec<u64>>();
    let per_producer = payloads_per_producer(producers);
    let received_count = Arc::new(AtomicU64::new(0));
    let received_checksum = Arc::new(AtomicU64::new(0));

    let consumer_handles: Vec<_> = (0..consumer_threads)
        .map(|i| {
            let rx = rx.clone();
            let received_count = Arc::clone(&received_count);
            let received_checksum = Arc::clone(&received_checksum);
            thread::spawn(move || {
                let mut local_count = 0u64;
                let mut local_checksum = 0u64;
                loop {
                    let mut batch = 0usize;
                    let target = if i == 0 {
                        CONSUMER0_BATCH
                    } else {
                        DRAIN_CONSUMER_BATCH
                    };
                    match rx.recv() {
                        Ok(payload) => {
                            local_checksum = local_checksum.wrapping_add(checksum(&payload));
                            local_count += 1;
                            batch += 1;
                        }
                        Err(_) => {
                            received_count.fetch_add(local_count, Ordering::Relaxed);
                            received_checksum.fetch_add(local_checksum, Ordering::Relaxed);
                            return;
                        }
                    }
                    while batch < target {
                        match rx.try_recv() {
                            Ok(payload) => {
                                local_checksum = local_checksum.wrapping_add(checksum(&payload));
                                local_count += 1;
                                batch += 1;
                            }
                            Err(_) => break,
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
    let consumer_threads = benchmark_parallelism();
    let (tx, rx) = kanal::unbounded::<Vec<u64>>();
    let per_producer = payloads_per_producer(producers);
    let received_count = Arc::new(AtomicU64::new(0));
    let received_checksum = Arc::new(AtomicU64::new(0));

    let consumer_handles: Vec<_> = (0..consumer_threads)
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
                                    received_checksum.fetch_add(local_checksum, Ordering::Relaxed);
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
                                received_checksum.fetch_add(local_checksum, Ordering::Relaxed);
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

fn bench_impl(c: &mut Criterion, backend: &str, producers: usize, run: fn(usize) -> RunTotals) {
    let consumer_threads = benchmark_parallelism();
    let (expected_count, expected_checksum) = expected_totals(producers);
    eprintln!(
        "running {backend}: {producers} producer threads, {consumer_threads} consumer threads, {expected_count} payloads"
    );

    let mut group = c.benchmark_group(benchmark_group_name(backend, producers));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(CRITERION_SAMPLES);
    group.measurement_time(measurement_budget());
    group.throughput(criterion::Throughput::Elements(expected_count as u64));
    group.bench_with_input(benchmark_id_channel(backend), &producers, |b, &producers| {
        b.iter(|| {
            let totals = run(producers);
            assert_eq!(totals.received as usize, expected_count);
            assert_eq!(totals.received_checksum, expected_checksum);
            black_box(totals);
        });
    });
    group.finish();
}

fn bench_feeder_impl(
    c: &mut Criterion,
    config: FeederBenchConfig,
    producers: usize,
) {
    let backend = format!("feeder_{}", config.suffix);
    let consumer_threads = benchmark_parallelism();
    let (expected_count, expected_checksum) = expected_totals(producers);
    eprintln!(
        "running {backend}: {producers} producer threads, {consumer_threads} consumer threads, {}, {expected_count} payloads",
        config_description(config)
    );

    let mut group = c.benchmark_group(benchmark_group_name(&backend, producers));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(CRITERION_SAMPLES);
    group.measurement_time(measurement_budget());
    group.throughput(criterion::Throughput::Elements(expected_count as u64));
    group.bench_with_input(benchmark_id_for_feeder(config), &producers, |b, &producers| {
        b.iter(|| {
            let totals = run_feeder_config(producers, config);
            assert_eq!(totals.received as usize, expected_count);
            assert_eq!(totals.received_checksum, expected_checksum);
            black_box(totals);
        });
    });
    group.finish();
}

fn tune_work_load() {
    eprintln!(
        "tuning: feeder_baseline, {TUNING_PRODUCERS} producers, {} consumers, {} total payloads",
        benchmark_parallelism(),
        work_total()
    );
    let start = Instant::now();
    let totals = run_feeder_config(TUNING_PRODUCERS, BENEFIT_MATRIX[0]);
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
        for config in BENEFIT_MATRIX {
            bench_feeder_impl(c, config, producers);
        }
        bench_impl(c, "crossbeam", producers, run_crossbeam);
        bench_impl(c, "crossbeam_drain", producers, run_crossbeam_drain);
        bench_impl(c, "kanal", producers, run_kanal);
    }
}

criterion_group!(benches, contention_benchmarks);
criterion_main!(benches);
