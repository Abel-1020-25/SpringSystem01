use std::{thread, time::Duration};

//  TASK 2 
fn track_changes() {
    let mut tracker = 0;

    let mut update = || {
        tracker += 1;
        println!("Tracker is now: {}", tracker);
    };

    update();
    update();
}

// TASK 3 (map + collect version)
fn process_vector_map<F>(vec: Vec<i32>, f: F) -> Vec<i32>
where
    F: Fn(i32) -> i32,
{
    vec.into_iter().map(f).collect()
}

//  TASK 3 (for loop version)
fn process_vector_loop<F>(vec: Vec<i32>, f: F) -> Vec<i32>
where
    F: Fn(i32) -> i32,
{
    let mut result = Vec::new();

    for num in vec {
        result.push(f(num));
    }

    result
}

//  TASK 5 
struct ComputeCache<T>
where
    T: Fn() -> String,
{
    computation: T,
    result: Option<String>,
}

impl<T> ComputeCache<T>
where
    T: Fn() -> String,
{
    fn new(computation: T) -> Self {
        ComputeCache {
            computation,
            result: None,
        }
    }

    fn get_result(&mut self) -> String {
        match &self.result {
            Some(value) => {
                println!("Retrieved from cache instantly!");
                value.clone()
            }
            None => {
                let value = (self.computation)();
                self.result = Some(value.clone());
                value
            }
        }
    }
}

fn main() {
    //  TASK 1 
    println!("Task 1:");
    let operation = |a: i32, b: i32| {
        a * b
    };
    println!("Result: {}", operation(10, 5));

    // TASK 2 
    println!("\nTask 2:");
    track_changes();

    // TASK 3 using map + collect
    println!("\nTask 3 using map and collect:");
    let numbers1 = vec![1, 2, 3];

    let doubled1 = process_vector_map(numbers1.clone(), |x| {
        x * 2
    });

    let replaced1 = process_vector_map(numbers1, |x| {
        if x > 2 {
            0
        } else {
            x
        }
    });

    println!("Doubled: {:?}", doubled1);
    println!("Replaced: {:?}", replaced1);

    // TASK 3 using for loop
    println!("\nTask 3 using for loop:");
    let numbers2 = vec![1, 2, 3];

    let doubled2 = process_vector_loop(numbers2.clone(), |x| {
        x * 2
    });

    let replaced2 = process_vector_loop(numbers2, |x| {
        if x > 2 {
            0
        } else {
            x
        }
    });

    println!("Doubled: {:?}", doubled2);
    println!("Replaced: {:?}", replaced2);

    // TASK 5
    println!("\nTask 5:");
    let mut cache = ComputeCache::new(|| {
        println!("Computing (this will take 2 seconds)...");
        thread::sleep(Duration::from_secs(2));
        "Hello, world!".to_string()
    });

    println!("First call:");
    println!("Result: {}", cache.get_result());

    println!("\nSecond call:");
    println!("Result (cached): {}", cache.get_result());
}