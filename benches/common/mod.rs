pub mod feeder_harness;

pub const PAYLOAD_LEN: usize = 64;
pub const WORK_TOTAL_PAYLOADS: usize = 14_500_000;

pub fn work_total() -> usize {
    if std::env::args().any(|a| a == "--test") {
        WORK_TOTAL_PAYLOADS / 200
    } else {
        WORK_TOTAL_PAYLOADS
    }
}

pub fn payloads_per_producer(producers: usize) -> usize {
    let total = if std::env::var("TUNE_WORK").is_ok() {
        WORK_TOTAL_PAYLOADS
    } else {
        work_total()
    };
    total / producers
}

pub fn make_payload(seed: u64) -> Vec<u64> {
    (0..PAYLOAD_LEN as u64)
        .map(|i| seed.wrapping_mul(31).wrapping_add(i))
        .collect()
}

pub fn checksum(payload: &[u64]) -> u64 {
    payload.iter().fold(0u64, |a, &b| a.wrapping_add(b))
}

pub fn payload_seed(producer: usize, seq: u64) -> u64 {
    (producer as u64) << 32 | seq
}

pub fn expected_totals(producers: usize) -> (usize, u64) {
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
