# Feeder performance investigation

## Current scheduler model

`FeederBuilder::scheduler_threads(n)` starts `n` scheduler worker threads. Each worker receives demand commands from the shared demand channel, pulls items from the shared recovered or ingress channels, and sends those items to the requested receiver.

The default decomposition benchmark uses the stable baseline of 4 scheduler threads. Set `FEEDER_SCHED_SWEEP=1` to compare 4, 8, and max-parallelism scheduler threads.

```bash
cargo bench --features perf-stats --bench decompose
cargo bench --features perf-stats --bench decompose -- --test
FEEDER_SCHED_SWEEP=1 cargo bench --features perf-stats --bench decompose -- --test
```

The contention benchmark uses 4 scheduler threads for every feeder variant and runs the full benefit matrix (8 configs) at each producer count:

```bash
cargo bench --features perf-stats --bench contention
FEEDER_STATS=1 cargo bench --features perf-stats --bench contention -- feeder_combined/producers=10
cargo bench --features perf-stats --bench contention -- crossbeam/producers=10
```

## Benefit matrix (what “combined” means)

All feeder bench cases use **4 scheduler workers**. “Combined” is not extra scheduler threads; it stacks three usage knobs:

| Suffix | Water marks (c0 / others) | Producers | Consumers 1..N |
|--------|---------------------------|-----------|----------------|
| `baseline` | 4/9, 1/3 | `send` | `get_one` |
| `tuned_water` | 16/64, 8/32 | `send` | `get_one` |
| `tx_batch` | 4/9, 1/3 | `send_batch(32)` | `get_one` |
| `consumer_drain` | 4/9, 1/3 | `send` | `get(max=8)` |
| `water_tx` | 16/64, 8/32 | `send_batch(32)` | `get_one` |
| `water_drain` | 16/64, 8/32 | `send` | `get(max=8)` |
| `tx_drain` | 4/9, 1/3 | `send_batch(32)` | `get(max=8)` |
| `combined` | 16/64, 8/32 | `send_batch(32)` | `get(max=8)` |

Consumer 0 always uses `get(max=3)` in feeder cases.

Shared harness: [`benches/common/feeder_harness.rs`](../benches/common/feeder_harness.rs) (`BENEFIT_MATRIX`).

## Receiver water marks

`Feeder::rx(low, high)` creates a receiver with refill hysteresis. The implementation tracks receiver capacity with `ReceiverHandle::outstanding`, not `data_rx.len()`.

- On registration: request `high` items.
- After consumption: subtract consumed items from `outstanding`.
- Below `low`: request `high - outstanding` more items.
- At or above `low`: do not request refill.

## Decompose scenarios

| Bench | Topology | Isolates |
|-------|----------|----------|
| `feeder_s{S}_p{N}_c1` | S scheduler threads, N producers, 1 consumer | Scheduler and producer scaling with low fan-out |
| `feeder_s{S}_p10_c{C}` | S scheduler threads, 10 producers, C consumers | Baseline config at full consumer count |
| `feeder_s4_p10_c{C}_no_checksum` | Baseline, no checksum in consumers | Consumer CPU vs feeder overhead |
| `feeder_s4_p1_c{C}` | 1 producer, C consumers | Producer MP contention removed |
| `feeder_s4_p10_c{C}_{suffix}` | Benefit matrix row at C consumers | See matrix table (8 suffixes) |
| `ingress_mp/p10` | 10 producers, 1 crossbeam drain | Ingress ceiling without feeder routing |

**Benefit matrix registration:** all 8 suffixes at **`c1`** and **`c = available_parallelism()`**, `s4`, `p10` only.

Example group names: `feeder_s4_p10_c32_baseline`, `feeder_s4_p10_c32_combined`, `feeder_s4_p10_c1_tuned_water`.

`S` is `4` by default, or `4`, `8`, and max parallelism when `FEEDER_SCHED_SWEEP=1` is set. `C` is `std::thread::available_parallelism()`.

Set `FEEDER_STATS=1` to print scheduler counters on decompose matrix iterations (otherwise only wall time).

## Contention scenarios

| Backend | Topology |
|---------|----------|
| `feeder_{suffix}` | Matrix row at max consumer count, 4 scheduler threads |
| `crossbeam` | Single channel, all consumers |
| `crossbeam_drain` | Single channel, consumers 1..N drain up to 8 |
| `kanal` | Single channel |

Producer counts: `4`, `8`, `10`. Consumer count: `available_parallelism()` for every case.

Legacy names map to matrix suffixes: `feeder` → `feeder_baseline`, `feeder_combined` → `feeder_combined`.

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

### Scheduler and ingress

- `ingress_mp` throughput much higher than `feeder_s*_p10_c*` means feeder routing, not producer `send`, is the cap.
- `feeder_s4_*` similar to `feeder_s8_*` means extra scheduler workers are not helping; shared routing work remains.
- `feeder_s4_p1_c*` similar to `feeder_s4_p10_c*` means producer-side multi-producer contention is not the limiter.

### Benefit matrix (compare at same `c` and `p10`)

- **Large spread at `c{C}` but not at `c1`:** wins are multi-consumer refill/demand traffic, not raw ingress.
- **`tuned_water` ≫ `baseline` at high `C`:** low default water marks (`1/3`) dominate demand-channel cost.
- **`tx_batch` ≈ `baseline` at high `C`:** producer atomics are not the cap once routing/refill dominates.
- **`consumer_drain` alone modest; `water_drain` or `combined` large:** drain helps mainly with larger refills.
- **`water_drain` ≈ `combined`:** water marks account for most of the combined gain; batch send is incremental.
- **Pair vs single gaps:** shows whether knobs are additive or overlapping.

### Other

- `feeder_s4_p10_c*_no_checksum` similar to baseline means consumer checksum CPU is not the limiter.
- High `refill_messages / items_consumed` means low water marks and `get_one()` are creating demand-channel pressure.
- Nonzero pending counters at end of a completed run indicate a lifecycle accounting bug.

## Runtime

- **Contention:** 8 feeder backends × 3 producer counts ≈ 4× the previous feeder portion (plus crossbeam/kanal).
- **Decompose matrix:** 8 suffixes × 2 consumer counts (`c1`, `c{C}`) = 16 groups at `s4`/`p10`.
- Use `--test` (1/200 payloads) for quick matrix checks; full runs use ~14.5M payloads per iteration.

## Scale target

Do not use this workstation benchmark suite to simulate hundreds of receiver threads. Use CPU-sized consumer counts and counters to judge whether architecture choices would block scale later.

Scheduler scaling (`FEEDER_SCHED_SWEEP=1`) is opt-in; treat high thread counts as correctness investigations first.
