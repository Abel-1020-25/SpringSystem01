use std::thread;
use std::sync::{Arc, Mutex};

fn assignment1() {
    let mut handles = Vec::new();

    for i in 1..=3 {
        let handle = thread::spawn(move || {
            println!("Thread {} is running", i);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("All threads completed.");
}

fn assignment2() {
    let counter = Arc::new(Mutex::new(0));
    let mut handles = Vec::new();

    for _ in 0..5 {
        let counter_clone = Arc::clone(&counter);

        let handle = thread::spawn(move || {
            for _ in 0..10 {
                let mut num = counter_clone.lock().unwrap();
                *num += 1;
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let final_count = counter.lock().unwrap();
    println!("Final counter value: {}", *final_count);
}

fn main() {
    println!("Assignment 1:");
    assignment1();

    println!();

    println!("Assignment 2:");
    assignment2();
}