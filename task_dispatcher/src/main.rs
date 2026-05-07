
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::VecDeque;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const TOTAL_TASKS: usize = 500;
const WORKER_COUNT: usize =6;

#[derive(Clone, Debug)]
enum TaskKind {
    CPU,
    IO,
}

#[derive(Clone, Debug)]
struct Task {
    id: usize,
    arrival_time: u128,
    kind: TaskKind,
    duration: u64,
}

struct CompletedTask {
    task: Task,
    start_time: u128,
    finish_time: u128,
    worker_id: usize,
}

enum DispatcherEvent {
    NewTask(Task),
    WorkerReady(usize),
    GeneratorDone,
}

enum WorkerMessage {
    Run(Task),
    Shutdown,
}

fn main() {
    let start = Instant::now();

    let (event_tx, event_rx) = mpsc::channel::<DispatcherEvent>();
    let (done_tx, done_rx) = mpsc::channel::<CompletedTask>();

    let mut worker_senders = Vec::new();
    let mut worker_handles = Vec::new();

    for worker_id in 0..WORKER_COUNT {
        let (task_tx, task_rx) = mpsc::channel::<WorkerMessage>();
        worker_senders.push(task_tx);

        let worker_event_tx = event_tx.clone();
        let worker_done_tx = done_tx.clone();

        let handle = thread::spawn(move || {
            worker_thread(worker_id, task_rx, worker_event_tx, worker_done_tx, start);
        });

        worker_handles.push(handle);
    }
    let generator_tx = event_tx.clone();

    let generator_handle = thread::spawn(move || {
        generate_tasks(generator_tx, start);
    });

    let dispatcher_handle = thread::spawn(move || {
        dispatcher(event_rx, worker_senders);
    });

    drop(done_tx);

    let mut completed = Vec::new();

    for result in done_rx {
        completed.push(result);
    }

    generator_handle.join().unwrap();
    dispatcher_handle.join().unwrap();

    for handle in worker_handles {
        handle.join().unwrap();
    }

    print_stats(completed, start);
}

fn generate_tasks(event_tx: mpsc::Sender<DispatcherEvent>, start: Instant){
    let mut rng = StdRng::seed_from_u64(12345);

    for id in 0..TOTAL_TASKS {
        let kind = if rng.gen_bool(0.55) {
            TaskKind::CPU
        } else {
            TaskKind::IO
        };

        let duration = match kind {
            TaskKind::CPU => rng.gen_range(20..70),
            TaskKind::IO => rng.gen_range(40..120),

        };

        let task = Task {
            id,
            arrival_time: start.elapsed().as_millis(),
            kind,
            duration,
        };

        event_tx.send(DispatcherEvent::NewTask(task)).unwrap();

        let arrival_delay = rng.gen_range(1..5);
        thread::sleep(Duration::from_millis(arrival_delay));
    }
    event_tx.send(DispatcherEvent::GeneratorDone).unwrap();
}

fn dispatcher(
    event_rx: mpsc::Receiver<DispatcherEvent>,
    worker_senders: Vec<mpsc::Sender<WorkerMessage>>,
) {
    let mut cpu_queue: VecDeque<Task> = VecDeque::new();
    let mut io_queue: VecDeque<Task> = VecDeque::new();

    let mut generator_done = false;
    let mut worker_busy = vec![false; WORKER_COUNT];
    let mut worker_shutdown = vec![false; WORKER_COUNT];

    let mut turn = 0;

    loop {
        let event = event_rx.recv().unwrap();

        match event {
            DispatcherEvent::NewTask(task) => match task.kind {
                TaskKind::CPU => cpu_queue.push_back(task),
                TaskKind::IO => io_queue.push_back(task),
            },
            DispatcherEvent::WorkerReady(worker_id) => {
                worker_busy[worker_id] = false;

                let next_task = choose_next_task(&mut cpu_queue, &mut io_queue, &mut turn);

                if let Some(task) = next_task {
                    worker_busy[worker_id] = true;
                    worker_senders[worker_id]
                        .send(WorkerMessage::Run(task))
                        .unwrap();
                } else if generator_done {
                    worker_shutdown[worker_id] = true;
                    worker_senders[worker_id]
                        .send(WorkerMessage::Shutdown)
                        .unwrap();
                }
            }

            DispatcherEvent::GeneratorDone => {
                generator_done = true;
            }
        }
        if generator_done && cpu_queue.is_empty() && io_queue.is_empty() {
            for id in 0..WORKER_COUNT {
                if !worker_busy[id] && !worker_shutdown[id] {
                    worker_shutdown[id] = true;
                    worker_senders[id]
                        .send(WorkerMessage::Shutdown)
                        .unwrap();
                }
            }
        }
        if worker_shutdown.iter().all(|x| *x) {
            break;
        }
    }
}

fn choose_next_task(
    cpu_queue: &mut VecDeque<Task>,
    io_queue: &mut VecDeque<Task>,
    turn: &mut usize,
) -> Option<Task> {
    if cpu_queue.is_empty() && io_queue.is_empty() {
        return None;
    }

    if cpu_queue.is_empty() {
        return io_queue.pop_front();
    }

    if io_queue.is_empty() {
        return cpu_queue.pop_front();
    }

    let task = if *turn % 2 == 0 {
        cpu_queue.pop_front()
    } else {
        io_queue.pop_front()
    };

    *turn += 1;
    task
}

fn worker_thread(
    worker_id: usize,
    task_rx: mpsc::Receiver<WorkerMessage>,
    event_tx: mpsc::Sender<DispatcherEvent>,
    done_tx: mpsc::Sender<CompletedTask>,
    start: Instant,
) {
    event_tx
        .send(DispatcherEvent::WorkerReady(worker_id))
        .unwrap();

    loop {
        let message = task_rx.recv().unwrap();

        match message {
            WorkerMessage::Run(task) => {
                let start_time = start.elapsed().as_millis();

                thread::sleep(Duration::from_millis(task.duration));

                let finish_time = start.elapsed().as_millis();

                let completed = CompletedTask {
                    task,
                    start_time,
                    finish_time,
                    worker_id,
                };

                done_tx.send(completed).unwrap();

                event_tx
                    .send(DispatcherEvent::WorkerReady(worker_id))
                    .unwrap();
            }

            WorkerMessage::Shutdown => {
                break;
            }
        }
    }
}

fn print_stats(completed: Vec<CompletedTask>, start: Instant) {
    let makespan = start.elapsed().as_millis();

    let mut total_wait = 0;
    let mut total_turnaround = 0;
    let mut max_wait = 0;

    let mut cpu_count = 0;
    let mut io_count = 0;

    let mut total_work_time = 0;

    for item in &completed {
        let wait_time = item.start_time - item.task.arrival_time;
        let turnaround_time = item.finish_time - item.task.arrival_time;

        total_wait += wait_time;
        total_turnaround += turnaround_time;
        total_work_time += item.task.duration as u128;

        if wait_time > max_wait {
            max_wait = wait_time;
        }

        match item.task.kind {
            TaskKind::CPU => cpu_count += 1,
            TaskKind::IO => io_count += 1,
        }
    }

    let total = completed.len() as u128;

    println!("===== Concurrent Task Dispatcher Results =====");
    println!("Total tasks completed: {}", completed.len());
    println!("CPU tasks completed: {}", cpu_count);
    println!("IO tasks completed: {}", io_count);
    println!("Makespan: {} ms", makespan);
    println!("Average wait time: {} ms", total_wait / total);
    println!("Average turnaround time: {} ms", total_turnaround / total);
    println!("Max wait time: {} ms", max_wait);

    let utilization = (total_work_time as f64 / (makespan as f64 * WORKER_COUNT as f64)) * 100.0;
    println!("Worker utilization: {:.2}%", utilization);
}