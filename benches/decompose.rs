mod common;

use common::feeder_harness::{
    config_description, feeder_stats_enabled, run_feeder_bench, BENEFIT_MATRIX,
    FeederBenchConfig, RunTotals,
};
use common::{expected_totals, work_total};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};

const MATRIX_SCHEDULER_THREADS: usize = 4;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use common::{make_payload, payload_seed, payloads_per_producer};
use feeder::Feeder;

const BASELINE_SCHEDULER_THREADS: usize = 4;
const CRITERION_SAMPLES: usize = 10;
const MEASUREMENT_MARGIN: f64 = 1.2;
const TARGET_WALL_SECS: f64 = 10.0;

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
    eprintln!(
        "  benefit matrix at c1 vs c{consumer_threads}: large spread at c{consumer_threads} -> multi-consumer refill/demand"
    );
    eprintln!("  set FEEDER_STATS=1 to print scheduler counters per matrix iteration");
    eprintln!("  high refill_messages / items_consumed -> demand-channel pressure");
    eprintln!("  high demand_abandoned vs demand_requested -> receivers dropped or shutdown");
    eprintln!("  ingress_pending or demand_pending > 0 at end -> stuck shutdown counters");
    eprintln!();
}

fn print_stats(feeder: &Feeder<Vec<u64>>, label: &str) {
    feeder.stats_snapshot().print_stderr(label);
}

fn run_feeder_baseline(
    scheduler_threads: usize,
    producers: usize,
    consumer_threads: usize,
    with_checksum: bool,
) -> (RunTotals, Feeder<Vec<u64>>) {
    run_feeder_bench(
        BENEFIT_MATRIX[0],
        scheduler_threads,
        producers,
        consumer_threads,
        with_checksum,
    )
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
            let (totals, _feeder) = run_feeder_baseline(
                scheduler_threads,
                producers,
                consumer_threads,
                with_checksum,
            );
            assert_eq!(totals.received as usize, expected_count);
            if with_checksum {
                assert_eq!(totals.received_checksum, expected_checksum);
            }
            black_box(totals);
        });
    });
    group.finish();
}

fn bench_feeder_matrix_case(
    c: &mut Criterion,
    group_name: &str,
    scheduler_threads: usize,
    producers: usize,
    consumer_threads: usize,
    config: FeederBenchConfig,
) {
    let (expected_count, expected_checksum) = expected_totals(producers);
    eprintln!(
        "running {group_name}: {scheduler_threads} scheduler threads, {producers} producers, {consumer_threads} consumers, {}, {expected_count} payloads",
        config_description(config)
    );

    let mut group = c.benchmark_group(group_name);
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(CRITERION_SAMPLES);
    group.measurement_time(measurement_budget());
    group.throughput(criterion::Throughput::Elements(expected_count as u64));
    group.bench_function("run", |b| {
        b.iter(|| {
            let (totals, feeder) = run_feeder_bench(
                config,
                scheduler_threads,
                producers,
                consumer_threads,
                true,
            );
            assert_eq!(totals.received as usize, expected_count);
            assert_eq!(totals.received_checksum, expected_checksum);
            if feeder_stats_enabled() {
                let label = format!("{group_name}/run");
                print_stats(&feeder, &label);
            }
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
    let max_consumers = benchmark_parallelism();

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
            &format!("feeder_s{scheduler_threads}_p10_c{max_consumers}"),
            scheduler_threads,
            10,
            max_consumers,
            true,
        );
    }
    bench_feeder_case(
        c,
        &format!("feeder_s{BASELINE_SCHEDULER_THREADS}_p10_c{max_consumers}_no_checksum"),
        BASELINE_SCHEDULER_THREADS,
        10,
        max_consumers,
        false,
    );
    bench_feeder_case(
        c,
        &format!("feeder_s{BASELINE_SCHEDULER_THREADS}_p1_c{max_consumers}"),
        BASELINE_SCHEDULER_THREADS,
        1,
        max_consumers,
        true,
    );

    for &consumer_threads in &[1usize, max_consumers] {
        for config in BENEFIT_MATRIX {
            bench_feeder_matrix_case(
                c,
                &format!(
                    "feeder_s{MATRIX_SCHEDULER_THREADS}_p10_c{consumer_threads}_{}",
                    config.suffix
                ),
                MATRIX_SCHEDULER_THREADS,
                10,
                consumer_threads,
                config,
            );
        }
    }

    bench_ingress(c, 10);
}

criterion_group!(benches, decompose_benchmarks);
criterion_main!(benches);
