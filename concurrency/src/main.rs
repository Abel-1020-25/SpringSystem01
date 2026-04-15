use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use rand::Rng;

// Define a special value that will signal termination
const TERMINATION_SIGNAL: i32 = -1;

fn main() {
    // Number of items to produce
    const ITEM_COUNT: usize = 20;

    // create channel
    let (tx, rx) = mpsc::channel();

    // wrap receiver so multiple consumers can share it
    let shared_rx = Arc::new(Mutex::new(rx));

    let mut producer_handles = Vec::new();
    let mut consumer_handles = Vec::new();

    // Create 2 producer threads
    for i in 0..2 {
        let tx_clone = tx.clone();

        let handle = thread::spawn(move || {
            producer(i + 1, tx_clone, ITEM_COUNT);
        });

        producer_handles.push(handle);
    }

    // Create 3 consumer threads
    for i in 0..3 {
        let rx_clone = Arc::clone(&shared_rx);

        let handle = thread::spawn(move || {
            consumer(i + 1, rx_clone);
        });

        consumer_handles.push(handle);
    }

    // Wait for producers to finish first
    for handle in producer_handles {
        handle.join().unwrap();
    }

    // send one termination signal for each consumer
    for _ in 0..3 {
        tx.send(TERMINATION_SIGNAL).unwrap();
    }

    // Wait for consumers to finish
    for handle in consumer_handles {
        handle.join().unwrap();
    }

    println!("All items have been produced and consumed!");
}

// Implement producer function
fn producer(id: usize, tx: mpsc::Sender<i32>, item_count: usize) {
    let mut rng = rand::thread_rng();

    for _ in 0..item_count {
        let num = rng.gen_range(1..101);

        println!("Producer {} made number {}", id, num);
        tx.send(num).unwrap();

        thread::sleep(Duration::from_millis(200));
    }

    println!("Producer {} is done", id);
}

// Implement consumer function
fn consumer(id: usize, rx: Arc<Mutex<mpsc::Receiver<i32>>>) {
    loop {
        let value = {
            let receiver = rx.lock().unwrap();
            receiver.recv().unwrap()
        };

        if value == TERMINATION_SIGNAL {
            println!("Consumer {} got termination signal and is exiting", id);
            break;
        } else {
            println!("Consumer {} received {}", id, value);
            thread::sleep(Duration::from_millis(300));
        }
    }
}