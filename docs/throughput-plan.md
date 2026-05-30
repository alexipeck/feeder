# Throughput plan

Goal: make feeder throughput exceed the direct channel baselines while preserving the design goal of supporting many consumers.

The current `c10` benchmark shape still shows feeder routing as the limiter:

- `ingress_mp/p10` is the raw producer-to-channel ceiling and remains much faster than feeder.
- `s4` scheduler cases are faster than `s8` and `s10`, so the current shared scheduler does not benefit from more worker threads.
- `p1_c10` is close to `p10_c10`, so producer-side multi-producer contention is not the primary cap.
- `send_batch` amortizes producer lifecycle atomics and the `tx_batch` probe moved wall time in the right direction in test mode, but the scheduler demand counters stayed essentially unchanged. Producer batching is useful, but it does not remove the main routing bottleneck.
- `p10_c10_no_checksum` is close to `p10_c10`, so consumer checksum CPU is not the primary cap.
- `consumer_drain` reduced `get_calls` in test mode but did not materially reduce `refill_messages / items_consumed` or improve throughput. Consumer batching alone is not enough while low water marks keep demand commands small and frequent.
- `refill_messages / items_consumed` is still roughly `0.26-0.30` for max-parallelism consumers with default water marks, so demand traffic is a meaningful part of the cost.
- This workstation should not be used to simulate hundreds of receiver threads or active receivers directly. The local evidence should focus on whether scheduler design choices reduce or amplify the per-item cost that would block that scale later.
- `demand_requested / command` is roughly `6.3-6.4` for the one-consumer cases but only roughly `3.4-3.8` for the max-parallelism `c10` case. Scheduler batching has some room to help, but refill policy must create larger demand batches before batching can pay off fully.
- The tuned-water benchmark (`consumer0 low/high = 16/64`, other consumers `8/32`) cut `refill_messages / items_consumed` to roughly `0.04` and raised `demand_requested / command` to roughly `27` in test mode. Treat this as evidence that refill policy matters, not as a default policy change by itself.
- The combined benchmark (`send_batch`, tuned water marks, and consumer drain) reached roughly `0.03s` in test mode with `refill_messages / items_consumed` around `0.034` and `demand_requested / command` around `29`. That is much closer to `ingress_mp`, but still behind the raw channel baseline.
- In the full contention benchmark, `feeder_combined/producers=10/consumers=10` now completes around `0.01s` in test mode on this workstation, matching both `crossbeam/producers=10/consumers=10` and `crossbeam_drain/producers=10/consumers=10`. Scheduler batch fulfillment (non-blocking acquire bursts per demand command, batched `demand_pending` / `active_worker_items` / perf accounting, single `active_sends` hold per burst) closed the remaining router gap for this case.
- Lifecycle accounting counters now use acquire/release ordering instead of global sequential consistency where they do not publish data. This reduces unnecessary ordering pressure in the router path, but it is still a local optimization; the architecture still pays per-item accounting and routing costs.
- The default decompose benchmark should stay on the stable `s4` scheduler baseline. Scheduler-thread scaling is opt-in with `FEEDER_SCHED_SWEEP=1` because higher-thread runs have repeatedly failed to make timely progress on this workstation.

## Immediate work

### Producer path

Current issue: `FeederTx::send` still performs multiple shared atomic operations per item before entering the ingress channel.

Next steps:

- Keep completion checks out of the running hot path.
- Use `FeederTx::send_batch` when producers naturally have batches. It keeps the graceful-shutdown seal guarantee while amortizing `active_producer_sends`, ingress accounting, and perf-stat updates across the batch.
- Audit `active_producer_sends`: it exists to make graceful shutdown seal ingress safely, but it costs two atomics per single-item send. A replacement ingress close protocol would need to preserve the same no-new-sends-after-seal behavior.
- Measure a producer-only feeder case against `ingress_mp` after each change.
- Consider an internal producer-side buffer only if callers cannot naturally use `send_batch`; avoid hiding unbounded latency or memory growth behind single-item `send`.

### Scheduler path

Current issue: adding scheduler workers hurts throughput. That means shared queues, shared atomics, and per-item routing dominate before worker CPU is saturated.

Next steps:

- Batch fulfillment for a demand command: pull up to `count` items and send them to the receiver in a tight loop, updating shared counters once per batch where possible. (Implemented: `try_acquire_item` bursts after each blocking acquire, one `active_sends` hold per burst, `commit_fulfillment` for batched demand and worker-item accounting.)
- Avoid allocation-heavy batching. A probe that collected each demand command into a fresh `Vec<T>` did not improve the test-mode benchmarks, so useful batching needs a lower-overhead representation or a larger architectural change such as shard-local queues.
- Avoid per-item `active_worker_items`, `demand_pending`, and `delivered_inflight` updates when a batch can account for them together.
- Use the combined benchmark as the scheduler target: after producer and consumer pressure are reduced, the remaining delta to `ingress_mp` is the routing/accounting cost the scheduler must remove.
- Keep lifecycle state transitions strongly ordered, but avoid sequential consistency for plain counters unless a specific cross-thread ordering requirement needs it.
- Do not hold `active_sends` while waiting for ingress. Receiver drop waits for `active_sends == 0`, so a worker that holds it while blocked in item acquisition can deadlock receiver teardown.
- Split demand by receiver or shard instead of all workers competing on one demand channel.
- Evaluate a single scheduler thread with batch routing against multiple scheduler workers; only scale workers after shared contention is reduced.
- Investigate stalls in higher scheduler-thread topologies separately. A local test run stalled after beginning an `s8` case while comparable `s4` cases completed quickly, so those cases should not be used as throughput evidence until the progress issue is isolated.
- Do not add local hundreds-receiver stress cases as a proxy for production scale. Instead, keep local active thread counts near available parallelism and use scheduler counters to identify per-item costs that would prevent scale-out.

### Consumer path

Current issue: the benchmark is dominated by `get_one()` consumers with low water marks, which produces frequent refill messages.

Next steps:

- Keep `get_one()` allocation-free.
- Use the existing `get(max)` path when consumers can process small batches. The `consumer_drain` probe shows this lowers consumer calls, but it must be paired with larger water marks or receiver-local credit batching to reduce scheduler demand traffic.
- Raise or tune default water marks for `get_one()`-heavy receivers; `low=1, high=3` creates high refill traffic.
- Use `demand_requested / command` as the validation metric for refill changes. The current max-parallelism case averages only about 3.4-3.8 requested credits per scheduler demand command with default water marks.
- Compare the tuned-water case against the default `p10_c10` case before changing defaults. A useful result needs lower `refill_messages / items_consumed`, higher `demand_requested / command`, and better wall time or throughput.
- Consider receiver-local credit accounting that batches refill requests without crossing the shared demand channel every few consumed items.

## Architecture work

Beating crossbeam is unlikely with a central per-item router because crossbeam is already optimized for direct send/receive. Feeder needs to win by doing less shared work per delivered item, especially when there are many consumers.

Candidate direction:

1. Producers push to one or more ingress shards.
2. Receivers publish demand in batches.
3. Schedulers match ingress batches to receiver demand batches.
4. Counters and lifecycle state are updated per batch, not per item.
5. The system uses a small number of scheduler workers unless sharding removes shared contention.

The near-term performance target should be: `feeder_combined/producers=10/consumers=10` approaches and then beats `crossbeam/producers=10/consumers=10` and `crossbeam_drain/producers=10/consumers=10`. Use that as the local proxy for whether scheduler per-item cost is low enough to support larger deployments; do not add local hundreds-consumer scenarios on this workstation.
