mod support;

use support::{run_combined_uniform, ConsumerMode};

fn main() {
    run_combined_uniform(ConsumerMode::GetOne);
}
