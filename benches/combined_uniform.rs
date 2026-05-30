mod common;

use common::feeder_harness::{
    config_description, run_feeder_bench, COMBINED_UNIFORM_CONFIGS, CONSUMER0_BATCH,
    FeederBenchConfig, UNIFORM_WATER_HIGH, UNIFORM_WATER_LOW,
};
use common::{expected_totals, work_total};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use std::num::NonZeroUsize;
use std::thread;
use std::time::Duration;

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
    eprintln!("combined_uniform benchmark topology");
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
    eprintln!("  scheduler threads: {SCHEDULER_THREADS}");
    eprintln!(
        "  watermarks (all receivers): low={UNIFORM_WATER_LOW} high={UNIFORM_WATER_HIGH}"
    );
    eprintln!("  consumer 0 (all cases): get(max={CONSUMER0_BATCH})");
    eprintln!("  producer (all cases): send_batch(32)");
    eprintln!("  configs:");
    for config in COMBINED_UNIFORM_CONFIGS {
        eprintln!("    feeder_{}: {}", config.suffix, config_description(config));
    }
    eprintln!("  set FEEDER_STATS=1 for scheduler counters on feeder runs");
    eprintln!("  payload type: Vec<u64> len {}", common::PAYLOAD_LEN);
    eprintln!();
}

fn benchmark_group_name(config: FeederBenchConfig, producers: usize) -> String {
    format!(
        "feeder_{}/producers={producers}/consumers={}",
        config.suffix,
        benchmark_parallelism()
    )
}

fn benchmark_id_for_feeder(config: FeederBenchConfig) -> BenchmarkId {
    let consumer_threads = benchmark_parallelism();
    let consumer0 = format!(
        "c0:low={UNIFORM_WATER_LOW},high={UNIFORM_WATER_HIGH},get({CONSUMER0_BATCH})"
    );
    let rest = match config.consumer_mode {
        common::feeder_harness::ConsumerMode::GetOne => format!(
            "c1-{}:low={UNIFORM_WATER_LOW},high={UNIFORM_WATER_HIGH},get_one",
            consumer_threads - 1
        ),
        common::feeder_harness::ConsumerMode::DrainRest(max) => format!(
            "c1-{}:low={UNIFORM_WATER_LOW},high={UNIFORM_WATER_HIGH},get({max})",
            consumer_threads - 1
        ),
    };
    BenchmarkId::new(format!("{consumer0}/producer=send_batch(32)"), rest)
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
            "combined_uniform_{}/p{producers}",
            config.suffix
        ));
    }
    let _ = feeder;
    RunTotals {
        received: totals.received,
        received_checksum: totals.received_checksum,
    }
}

fn bench_feeder_impl(c: &mut Criterion, config: FeederBenchConfig, producers: usize) {
    let backend = format!("feeder_{}", config.suffix);
    let consumer_threads = benchmark_parallelism();
    let (expected_count, expected_checksum) = expected_totals(producers);
    eprintln!(
        "running {backend}: {producers} producer threads, {consumer_threads} consumer threads, {}, {expected_count} payloads",
        config_description(config)
    );

    let mut group = c.benchmark_group(benchmark_group_name(config, producers));
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

fn combined_uniform_benchmarks(c: &mut Criterion) {
    log_benchmark_topology();
    for &producers in &PRODUCER_COUNTS {
        for config in COMBINED_UNIFORM_CONFIGS {
            bench_feeder_impl(c, config, producers);
        }
    }
}

criterion_group!(benches, combined_uniform_benchmarks);
criterion_main!(benches);
