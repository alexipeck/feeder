use feeder::Feeder;
use std::num::NonZeroUsize;

fn main() {
    let feeder = Feeder::<i32>::builder().build();
    let rx = feeder
        .rx(NonZeroUsize::new(3).unwrap())
        .expect("receiver");
    let tx = feeder.tx().expect("sender");
    for i in 0..10 {
        tx.send(i).unwrap();
    }
    feeder.graceful_shutdown();
    drop(tx);
    let mut count = 0usize;
    loop {
        match rx.get_one() {
            Ok(_) => count += 1,
            Err(feeder::GetError::NoMoreWork) => break,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    println!("consumed {count} items");
}
