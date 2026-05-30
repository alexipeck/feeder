mod support;

use support::{run_combined_uniform, ConsumerMode, DRAIN_CONSUMER_BATCH};

fn main() {
    run_combined_uniform(ConsumerMode::DrainRest(DRAIN_CONSUMER_BATCH));
}
