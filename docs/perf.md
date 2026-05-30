# Feeder performance investigation

## Current scheduler model

`FeederBuilder::scheduler_threads(n)` starts `n` scheduler worker threads. Each worker receives demand commands from the shared demand channel, pulls items from the shared recovered or ingress channels, and sends those items to the requested receiver.

The default decomposition benchmark uses the stable baseline of 4 scheduler threads. Set `FEEDER_SCHED_SWEEP=1` to compare 4, 8, and max-parallelism scheduler threads. On this machine max parallelism is expected to be 10 threads.

```bash
cargo bench --features perf-stats --bench decompose
cargo bench --features perf-stats --bench decompose -- --test
FEEDER_SCHED_SWEEP=1 cargo bench --features perf-stats --bench decompose -- --test
```

The contention benchmark still uses 4 scheduler threads by default:

```bash
FEEDER_STATS=1 cargo bench --features perf-stats --bench contention -- feeder/producers=10
FEEDER_STATS=1 cargo bench --features perf-stats --bench contention -- feeder_combined/producers=10
cargo bench --features perf-stats --bench contention -- crossbeam/producers=10
cargo bench --features perf-stats --bench contention -- crossbeam_drain/producers=10
```

## Receiver water marks

`Feeder::rx(low, high)` creates a receiver with refill hysteresis. The implementation tracks receiver capacity with `ReceiverHandle::outstanding`, not `data_rx.len()`.

- On registration: request `high` items.
- After consumption: subtract consumed items from `outstanding`.
- Below `low`: request `high - outstanding` more items.
- At or above `low`: do not request refill.

Contention defaults:

- Consumer 0: `low=4`, `high=9`, `get(max=3)`.
- Consumers 1 through `available_parallelism() - 1`: `low=1`, `high=3`, `get_one()`.

The decompose benchmark also includes one tuned-water baseline at 4 scheduler threads:

- Consumer 0: `low=16`, `high=64`, `get(max=3)`.
- Consumers 1 through `available_parallelism() - 1`: `low=8`, `high=32`, `get_one()`.

This case is named `feeder_s4_p10_c{C}_tuned_water`. It tests whether larger refill requests reduce scheduler demand traffic enough to improve throughput. It is not a production default recommendation by itself; use the counters below to decide whether the gain comes from fewer demand commands, larger commands, or another effect.

Benchmarks use `std::thread::available_parallelism()` for the consumer count instead of a hardcoded topology. On this computer that should make the former `c32` cases run as `c10`.

## Decompose scenarios

| Bench | Topology | Isolates |
|-------|----------|----------|
| `feeder_s{S}_p{N}_c1` | S scheduler threads, N producers, 1 consumer | Scheduler, demand, and producer scaling with low fan-out |
| `feeder_s{S}_p10_c{C}` | S scheduler threads, 10 producers, C consumers | Full contention with checksum work |
| `feeder_s4_p10_c{C}_no_checksum` | Baseline scheduler size, no checksum in consumers | Consumer CPU vs feeder overhead |
| `feeder_s4_p1_c{C}` | 4 scheduler threads, 1 producer, C consumers | Producer MP contention removed at the baseline scheduler size |
| `feeder_s4_p10_c{C}_tuned_water` | 4 scheduler threads, larger refill water marks | Demand-channel pressure from low water marks |
| `feeder_s4_p10_c{C}_tx_batch` | 4 scheduler threads, producers use `FeederTx::send_batch` with 32 items | Producer hot-path atomic overhead |
| `feeder_s4_p10_c{C}_consumer_drain` | 4 scheduler threads, consumers 1..C use `get(max=8)` | Consumer call overhead vs refill traffic |
| `feeder_s4_p10_c{C}_combined` | Tuned water marks, producer batch sends, consumer drain | Whether the isolated improvements compound |
| `ingress_mp/p10` | 10 producers, 1 crossbeam drain | Ingress channel ceiling without feeder routing |

`S` is `4` by default, or `4`, `8`, and max parallelism when `FEEDER_SCHED_SWEEP=1` is set. `C` is `std::thread::available_parallelism()`.

## Reading counters

Scheduler counters:

- `worker_count`: scheduler worker threads configured for the feeder.
- `ingress_accepted`: producer sends accepted into feeder ingress.
- `ingress_consumed`: items workers pulled from ingress.
- `demand_commands`: receiver demand commands submitted to the scheduler.
- `demand_requested`: total receiver demand credits requested.
- `demand_fulfilled`: demand credits satisfied by a send to a receiver.
- `demand_abandoned`: demand credits discarded during receiver close or shutdown.
- `recovered_enqueued`: items recovered from dropped receivers.
- `recovered_routed`: recovered items routed to a receiver.
- `output_routed`: items sent to receiver output channels.
- `receiver_drop_recovered`: buffered items recovered when receivers dropped.
- `ingress_pending`, `demand_pending`, `recovered_pending`, `active_worker_items`, `delivered_inflight`: end-of-run lifecycle counters; these should normally finish at zero.

Consumer counters:

- `get_calls`: receiver `get`/`get_one` calls that consumed at least one item.
- `items_consumed`: items returned to consumers.
- `refill_messages`: refill requests emitted after consumption.

Useful ratios:

- `refill_messages / items_consumed`: demand traffic per consumed item.
- `demand_requested / command`: average demand command size; low values mean scheduler batching has little to work with.
- `output_routed / items_consumed`: should stay near 1.0.
- `demand_abandoned / demand_requested`: should be small except for normal shutdown tail demand.

## Interpretation

- `ingress_mp` throughput much higher than `feeder_s*_p10_c*` means feeder routing, not producer `send`, is the cap.
- `feeder_s4_*` similar to `feeder_s8_*` and max-parallelism scheduler cases means extra scheduler workers are not helping; shared channel, atomic, or per-item routing work is likely the limiter.
- `feeder_s8_*` or max-parallelism scheduler cases faster than `feeder_s4_*` means scheduler worker count was part of the limiter.
- `feeder_s4_p1_c*` similar to `feeder_s4_p10_c*` means producer-side multi-producer contention is not the limiter at the baseline scheduler size.
- `feeder_s4_p10_c*_tx_batch` faster than the default producer case means producer send overhead matters. If scheduler demand counters remain similar, the remaining limiter is still downstream routing and demand pressure.
- `feeder_s4_p10_c*_no_checksum` similar to `feeder_s4_p10_c*` means consumer checksum CPU is not the limiter at the baseline scheduler size.
- `feeder_s4_p10_c*_consumer_drain` reducing `get_calls` without reducing `refill_messages / items_consumed` means the consumer call loop is not the root cause; refill hysteresis and scheduler demand traffic remain the cap.
- `feeder_s4_p10_c*_tuned_water` faster than the default water-mark case means refill policy is contributing materially to the cap. If it only improves `demand_requested / command` without improving wall time, scheduler routing still has enough per-item shared work to dominate.
- `feeder_s4_p10_c*_combined` closing much of the gap to `ingress_mp` means producer batching, larger refill commands, and consumer drain behavior compound. Any remaining gap after this case is mostly scheduler/router overhead.
- High `refill_messages / items_consumed` means low water marks and `get_one()` usage are creating demand-channel pressure.
- Nonzero pending counters at end of a completed run indicate a lifecycle accounting bug, not just low throughput.

## Scale target

Do not use this workstation benchmark suite to simulate hundreds of receiver threads or hundreds of active receivers. The hundreds-consumer requirement is a design target: the scheduler should avoid architecture choices that would prevent that scale once deployed on hardware sized for it.

Local benchmarks should use CPU-sized active thread counts and decompose scheduler costs with counters. Treat those results as evidence about whether the scheduler can eventually provide enough throughput at larger scale, not as a direct local simulation of hundreds of consumers.

Scheduler scaling cases are opt-in because higher scheduler-thread test-mode runs have repeatedly failed to make timely progress on this workstation. Treat those runs as progress/correctness investigations first, and throughput evidence only after they complete reliably.

## Current crossbeam comparison

The optimized contention case is `feeder_combined/producers=10/consumers=10`: tuned water marks, producer `send_batch(32)`, and consumer `get(max=8)` for non-primary consumers.

In test mode on this workstation, `feeder_combined` completes around `0.01s`, matching both `crossbeam/producers=10` and `crossbeam_drain/producers=10`. The feeder counters in that case stay healthy (`refill_messages / items_consumed` around `0.034`, `demand_requested / command` around `29`, no pending counters at end). Default-water `feeder` cases remain slower; the combined configuration is the current throughput reference.
