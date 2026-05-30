mod common;

use common::{
    checksum, expected_totals, make_payload, payload_seed, payloads_per_producer, work_total,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use feeder::Feeder;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const CONSUMER0_BATCH: usize = 3;
const FEEDER_CONSUMER0_LOW: usize = 4;
const FEEDER_CONSUMER0_HIGH: usize = 9;
const FEEDER_OTHER_LOW: usize = 1;
const FEEDER_OTHER_HIGH: usize = 3;
const TUNED_CONSUMER0_LOW: usize = 16;
const TUNED_CONSUMER0_HIGH: usize = 64;
const TUNED_OTHER_LOW: usize = 8;
const TUNED_OTHER_HIGH: usize = 32;
const BASELINE_SCHEDULER_THREADS: usize = 4;
const FEEDER_SEND_BATCH: usize = 32;
const DRAIN_CONSUMER_BATCH: usize = 8;
const CRITERION_SAMPLES: usize = 10;
const MEASUREMENT_MARGIN: f64 = 1.2;

fn benchmark_parallelism() -> usize {
    thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1)
}

fn scheduler_thread_counts() -> Vec<usize> {
    if std::env::var("FEEDER_SCHED_SWEEP").is_err() {
        return vec![BASELINE_SCHEDULER_THREADS];
    }
    let mut counts = vec![4, 8, benchmark_parallelism()];
    counts.sort_unstable();
    counts.dedup();
    counts
}

fn measurement_budget() -> Duration {
    if std::env::args().any(|a| a == "--test") {
        Duration::from_secs(5)
    } else {
        Duration::from_secs_f64(TARGET_WALL_SECS * MEASUREMENT_MARGIN * CRITERION_SAMPLES as f64)
    }
}

const TARGET_WALL_SECS: f64 = 10.0;

fn log_cheatsheet() {
    let consumer_threads = benchmark_parallelism();
    eprintln!();
    eprintln!("decompose bench interpretation:");
    eprintln!(
        "  ingress_mp thrpt >> feeder_s*_p10_c{consumer_threads} -> bottleneck is feeder routing, not crossbeam send"
    );
    eprintln!(
        "  set FEEDER_SCHED_SWEEP=1 to include s8/s{consumer_threads} scheduler scaling cases"
    );
    eprintln!(
        "  feeder_s4 thrpt ~ feeder_s8/s{consumer_threads} -> shared scheduler contention or serial work remains"
    );
    eprintln!(
        "  feeder_s8/s{consumer_threads} faster than feeder_s4 -> scheduler workers were the cap"
    );
    eprintln!(
        "  feeder_s*_p4_c1 thrpt ~ feeder_s*_p10_c1 at same scheduler count -> producer count is not the cap"
    );
    eprintln!("  high refill_messages / items_consumed -> demand-channel pressure");
    eprintln!("  high demand_abandoned vs demand_requested -> receivers dropped or shutdown");
    eprintln!("  ingress_pending or demand_pending > 0 at end -> stuck shutdown counters");
    eprintln!();
}

struct RunTotals {
    received: u64,
    received_checksum: u64,
}

#[derive(Clone, Copy)]
enum ProducerMode {
    SingleSend,
    BatchSend(usize),
}

#[derive(Clone, Copy)]
enum ConsumerMode {
    Current,
    DrainRest(usize),
}

#[derive(Clone, Copy)]
struct WaterMarks {
    consumer0_low: usize,
    consumer0_high: usize,
    other_low: usize,
    other_high: usize,
}

const DEFAULT_WATER: WaterMarks = WaterMarks {
    consumer0_low: FEEDER_CONSUMER0_LOW,
    consumer0_high: FEEDER_CONSUMER0_HIGH,
    other_low: FEEDER_OTHER_LOW,
    other_high: FEEDER_OTHER_HIGH,
};

const TUNED_WATER: WaterMarks = WaterMarks {
    consumer0_low: TUNED_CONSUMER0_LOW,
    consumer0_high: TUNED_CONSUMER0_HIGH,
    other_low: TUNED_OTHER_LOW,
    other_high: TUNED_OTHER_HIGH,
};

fn print_stats(feeder: &Feeder<Vec<u64>>, label: &str) {
    feeder.stats_snapshot().print_stderr(label);
}

fn spawn_feeder_consumers(
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
                            ConsumerMode::Current => match rx.get_one() {
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

fn run_feeder(
    scheduler_threads: usize,
    producers: usize,
    consumer_threads: usize,
    with_checksum: bool,
) -> (RunTotals, Feeder<Vec<u64>>) {
    run_feeder_with_water(
        scheduler_threads,
        producers,
        consumer_threads,
        with_checksum,
        DEFAULT_WATER,
        ProducerMode::SingleSend,
        ConsumerMode::Current,
    )
}

fn run_feeder_with_water(
    scheduler_threads: usize,
    producers: usize,
    consumer_threads: usize,
    with_checksum: bool,
    water: WaterMarks,
    producer_mode: ProducerMode,
    consumer_mode: ConsumerMode,
) -> (RunTotals, Feeder<Vec<u64>>) {
    let wall_start = Instant::now();
    let feeder = Feeder::<Vec<u64>>::builder()
        .scheduler_threads(NonZeroUsize::new(scheduler_threads).unwrap())
        .build();
    feeder.reset_stats();
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
        consumer_mode,
        Arc::clone(&received_count),
        Arc::clone(&received_checksum),
    );

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

fn run_ingress_mp(producers: usize) -> RunTotals {
    let wall_start = Instant::now();
    let (tx, rx) = crossbeam_channel::unbounded::<Vec<u64>>();
    let per_producer = payloads_per_producer(producers);
    let received_count = Arc::new(AtomicU64::new(0));

    let drain = {
        let rx = rx.clone();
        let received_count = Arc::clone(&received_count);
        thread::spawn(move || {
            let mut local = 0u64;
            while rx.recv().is_ok() {
                local += 1;
            }
            received_count.fetch_add(local, Ordering::Relaxed);
        })
    };

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
    drain.join().unwrap();

    let totals = RunTotals {
        received: received_count.load(Ordering::Relaxed),
        received_checksum: 0,
    };
    eprintln!(
        "  wall {:.2}s, {} payloads",
        wall_start.elapsed().as_secs_f64(),
        totals.received
    );
    totals
}

fn bench_feeder_case(
    c: &mut Criterion,
    group_name: &str,
    scheduler_threads: usize,
    producers: usize,
    consumer_threads: usize,
    with_checksum: bool,
) {
    let (expected_count, expected_checksum) = expected_totals(producers);
    eprintln!(
        "running {group_name}: {scheduler_threads} scheduler threads, {producers} producers, {consumer_threads} consumers, {expected_count} payloads"
    );

    let mut group = c.benchmark_group(group_name);
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(CRITERION_SAMPLES);
    group.measurement_time(measurement_budget());
    group.throughput(criterion::Throughput::Elements(expected_count as u64));
    group.bench_function("run", |b| {
        b.iter(|| {
            let (totals, feeder) = run_feeder(
                scheduler_threads,
                producers,
                consumer_threads,
                with_checksum,
            );
            assert_eq!(totals.received as usize, expected_count);
            if with_checksum {
                assert_eq!(totals.received_checksum, expected_checksum);
            }
            let label = format!("{group_name}/run");
            print_stats(&feeder, &label);
            black_box(totals);
        });
    });
    group.finish();
}

fn bench_feeder_water_case(
    c: &mut Criterion,
    group_name: &str,
    scheduler_threads: usize,
    producers: usize,
    consumer_threads: usize,
    with_checksum: bool,
    water: WaterMarks,
) {
    let (expected_count, expected_checksum) = expected_totals(producers);
    eprintln!(
        "running {group_name}: {scheduler_threads} scheduler threads, {producers} producers, {consumer_threads} consumers, water c0={}:{}, other={}:{}, {expected_count} payloads",
        water.consumer0_low, water.consumer0_high, water.other_low, water.other_high
    );

    let mut group = c.benchmark_group(group_name);
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(CRITERION_SAMPLES);
    group.measurement_time(measurement_budget());
    group.throughput(criterion::Throughput::Elements(expected_count as u64));
    group.bench_function("run", |b| {
        b.iter(|| {
            let (totals, feeder) = run_feeder_with_water(
                scheduler_threads,
                producers,
                consumer_threads,
                with_checksum,
                water,
                ProducerMode::SingleSend,
                ConsumerMode::Current,
            );
            assert_eq!(totals.received as usize, expected_count);
            if with_checksum {
                assert_eq!(totals.received_checksum, expected_checksum);
            }
            let label = format!("{group_name}/run");
            print_stats(&feeder, &label);
            black_box(totals);
        });
    });
    group.finish();
}

fn bench_feeder_batch_producer_case(
    c: &mut Criterion,
    group_name: &str,
    scheduler_threads: usize,
    producers: usize,
    consumer_threads: usize,
    with_checksum: bool,
) {
    let (expected_count, expected_checksum) = expected_totals(producers);
    eprintln!(
        "running {group_name}: {scheduler_threads} scheduler threads, {producers} producers, {consumer_threads} consumers, producer batch={FEEDER_SEND_BATCH}, {expected_count} payloads"
    );

    let mut group = c.benchmark_group(group_name);
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(CRITERION_SAMPLES);
    group.measurement_time(measurement_budget());
    group.throughput(criterion::Throughput::Elements(expected_count as u64));
    group.bench_function("run", |b| {
        b.iter(|| {
            let (totals, feeder) = run_feeder_with_water(
                scheduler_threads,
                producers,
                consumer_threads,
                with_checksum,
                DEFAULT_WATER,
                ProducerMode::BatchSend(FEEDER_SEND_BATCH),
                ConsumerMode::Current,
            );
            assert_eq!(totals.received as usize, expected_count);
            if with_checksum {
                assert_eq!(totals.received_checksum, expected_checksum);
            }
            let label = format!("{group_name}/run");
            print_stats(&feeder, &label);
            black_box(totals);
        });
    });
    group.finish();
}

fn bench_feeder_consumer_drain_case(
    c: &mut Criterion,
    group_name: &str,
    scheduler_threads: usize,
    producers: usize,
    consumer_threads: usize,
    with_checksum: bool,
) {
    let (expected_count, expected_checksum) = expected_totals(producers);
    eprintln!(
        "running {group_name}: {scheduler_threads} scheduler threads, {producers} producers, {consumer_threads} consumers, rest consumer drain={DRAIN_CONSUMER_BATCH}, {expected_count} payloads"
    );

    let mut group = c.benchmark_group(group_name);
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(CRITERION_SAMPLES);
    group.measurement_time(measurement_budget());
    group.throughput(criterion::Throughput::Elements(expected_count as u64));
    group.bench_function("run", |b| {
        b.iter(|| {
            let (totals, feeder) = run_feeder_with_water(
                scheduler_threads,
                producers,
                consumer_threads,
                with_checksum,
                DEFAULT_WATER,
                ProducerMode::SingleSend,
                ConsumerMode::DrainRest(DRAIN_CONSUMER_BATCH),
            );
            assert_eq!(totals.received as usize, expected_count);
            if with_checksum {
                assert_eq!(totals.received_checksum, expected_checksum);
            }
            let label = format!("{group_name}/run");
            print_stats(&feeder, &label);
            black_box(totals);
        });
    });
    group.finish();
}

fn bench_feeder_combined_case(
    c: &mut Criterion,
    group_name: &str,
    scheduler_threads: usize,
    producers: usize,
    consumer_threads: usize,
    with_checksum: bool,
) {
    let (expected_count, expected_checksum) = expected_totals(producers);
    eprintln!(
        "running {group_name}: {scheduler_threads} scheduler threads, {producers} producers, {consumer_threads} consumers, tuned water, producer batch={FEEDER_SEND_BATCH}, rest consumer drain={DRAIN_CONSUMER_BATCH}, {expected_count} payloads"
    );

    let mut group = c.benchmark_group(group_name);
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(CRITERION_SAMPLES);
    group.measurement_time(measurement_budget());
    group.throughput(criterion::Throughput::Elements(expected_count as u64));
    group.bench_function("run", |b| {
        b.iter(|| {
            let (totals, feeder) = run_feeder_with_water(
                scheduler_threads,
                producers,
                consumer_threads,
                with_checksum,
                TUNED_WATER,
                ProducerMode::BatchSend(FEEDER_SEND_BATCH),
                ConsumerMode::DrainRest(DRAIN_CONSUMER_BATCH),
            );
            assert_eq!(totals.received as usize, expected_count);
            if with_checksum {
                assert_eq!(totals.received_checksum, expected_checksum);
            }
            let label = format!("{group_name}/run");
            print_stats(&feeder, &label);
            black_box(totals);
        });
    });
    group.finish();
}

fn bench_ingress(c: &mut Criterion, producers: usize) {
    let (expected_count, _) = expected_totals(producers);
    let group_name = format!("ingress_mp/p{producers}");
    eprintln!(
        "running {group_name}: {producers} producers, 1 drain thread, {expected_count} payloads"
    );

    let mut group = c.benchmark_group(&group_name);
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(CRITERION_SAMPLES);
    group.measurement_time(measurement_budget());
    group.throughput(criterion::Throughput::Elements(expected_count as u64));
    group.bench_function("run", |b| {
        b.iter(|| {
            let totals = run_ingress_mp(producers);
            assert_eq!(totals.received as usize, expected_count);
            black_box(totals);
        });
    });
    group.finish();
}

fn decompose_benchmarks(c: &mut Criterion) {
    log_cheatsheet();
    eprintln!(
        "decompose workload: {} total payloads (use --test for 1/200)",
        work_total()
    );
    let consumer_threads = benchmark_parallelism();

    for scheduler_threads in scheduler_thread_counts() {
        for &producers in &[4usize, 8, 10] {
            bench_feeder_case(
                c,
                &format!("feeder_s{scheduler_threads}_p{producers}_c1"),
                scheduler_threads,
                producers,
                1,
                true,
            );
        }

        bench_feeder_case(
            c,
            &format!("feeder_s{scheduler_threads}_p10_c{consumer_threads}"),
            scheduler_threads,
            10,
            consumer_threads,
            true,
        );
    }
    bench_feeder_case(
        c,
        &format!("feeder_s{BASELINE_SCHEDULER_THREADS}_p10_c{consumer_threads}_no_checksum"),
        BASELINE_SCHEDULER_THREADS,
        10,
        consumer_threads,
        false,
    );
    bench_feeder_case(
        c,
        &format!("feeder_s{BASELINE_SCHEDULER_THREADS}_p1_c{consumer_threads}"),
        BASELINE_SCHEDULER_THREADS,
        1,
        consumer_threads,
        true,
    );
    bench_feeder_water_case(
        c,
        &format!("feeder_s{BASELINE_SCHEDULER_THREADS}_p10_c{consumer_threads}_tuned_water"),
        BASELINE_SCHEDULER_THREADS,
        10,
        consumer_threads,
        true,
        TUNED_WATER,
    );
    bench_feeder_batch_producer_case(
        c,
        &format!("feeder_s{BASELINE_SCHEDULER_THREADS}_p10_c{consumer_threads}_tx_batch"),
        BASELINE_SCHEDULER_THREADS,
        10,
        consumer_threads,
        true,
    );
    bench_feeder_consumer_drain_case(
        c,
        &format!("feeder_s{BASELINE_SCHEDULER_THREADS}_p10_c{consumer_threads}_consumer_drain"),
        BASELINE_SCHEDULER_THREADS,
        10,
        consumer_threads,
        true,
    );
    bench_feeder_combined_case(
        c,
        &format!("feeder_s{BASELINE_SCHEDULER_THREADS}_p10_c{consumer_threads}_combined"),
        BASELINE_SCHEDULER_THREADS,
        10,
        consumer_threads,
        true,
    );
    bench_ingress(c, 10);
}

criterion_group!(benches, decompose_benchmarks);
criterion_main!(benches);
