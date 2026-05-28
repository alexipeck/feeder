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
const CRITERION_SAMPLES: usize = 10;
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

const TARGET_WALL_SECS: f64 = 10.0;

fn log_cheatsheet() {
    eprintln!();
    eprintln!("decompose bench interpretation:");
    eprintln!("  ingress_mp thrpt >> feeder_p10_c32 -> bottleneck is feeder routing, not crossbeam send");
    eprintln!("  feeder_p4 thrpt ~ feeder_p10 -> scheduler serial cap");
    eprintln!("  high refill_messages / items_consumed -> demand-channel pressure");
    eprintln!("  high head_stale or head_advanced vs ingress_routed -> demand rotation overhead");
    eprintln!("  select_idle_timeouts > 0 under load -> unexpected idle polling");
    eprintln!();
}

struct RunTotals {
    received: u64,
    received_checksum: u64,
}

fn print_stats(feeder: &Feeder<Vec<u64>>, label: &str) {
    feeder.stats_snapshot().print_stderr(label);
}

fn spawn_feeder_consumers(
    receivers: Vec<feeder::FeederRx<Vec<u64>>>,
    with_checksum: bool,
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
                        match rx.get_one() {
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
    producers: usize,
    consumer_threads: usize,
    with_checksum: bool,
) -> (RunTotals, Feeder<Vec<u64>>) {
    let wall_start = Instant::now();
    let feeder = Feeder::<Vec<u64>>::builder().build();
    feeder.reset_stats();
    let water0 = (
        NonZeroUsize::new(FEEDER_CONSUMER0_LOW).unwrap(),
        NonZeroUsize::new(FEEDER_CONSUMER0_HIGH).unwrap(),
    );
    let water_rest = (
        NonZeroUsize::new(FEEDER_OTHER_LOW).unwrap(),
        NonZeroUsize::new(FEEDER_OTHER_HIGH).unwrap(),
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
        Arc::clone(&received_count),
        Arc::clone(&received_checksum),
    );

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
    producers: usize,
    consumer_threads: usize,
    with_checksum: bool,
) {
    let (expected_count, expected_checksum) = expected_totals(producers);
    eprintln!(
        "running {group_name}: {producers} producers, {consumer_threads} consumers, {expected_count} payloads"
    );

    let mut group = c.benchmark_group(group_name);
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(CRITERION_SAMPLES);
    group.measurement_time(measurement_budget());
    group.throughput(criterion::Throughput::Elements(expected_count as u64));
    group.bench_function("run", |b| {
        b.iter(|| {
            let (totals, feeder) = run_feeder(producers, consumer_threads, with_checksum);
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
    eprintln!("running {group_name}: {producers} producers, 1 drain thread, {expected_count} payloads");

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

    for &producers in &[4usize, 8, 10] {
        bench_feeder_case(
            c,
            &format!("feeder_p{producers}_c1"),
            producers,
            1,
            true,
        );
    }

    bench_feeder_case(c, "feeder_p10_c32", 10, 32, true);
    bench_feeder_case(c, "feeder_p10_c32_no_checksum", 10, 32, false);
    bench_feeder_case(c, "feeder_p1_c32", 1, 32, true);
    bench_ingress(c, 10);
}

criterion_group!(benches, decompose_benchmarks);
criterion_main!(benches);
