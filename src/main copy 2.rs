use std::thread;
use std::time::Duration;

fn main() {

    // Spawn a new thread
    let handle1 = thread::spawn(|| {
        for i in 1..5 {
            println!("Hello from the spawned thread 1: {}", i);
            thread::sleep(Duration::from_millis(500));
        }
    });

    // Spawn a new thread
    let handle2 = thread::spawn(|| {
        for i in 1..5 {
            println!("Hello from the spawned thread 2: {}", i);
            thread::sleep(Duration::from_millis(500));
        }
    });

    // Main thread continues
    for i in 1..5 {
        println!("Hello from the main thread: {}", i);
        thread::sleep(Duration::from_millis(500));
    }

    // Wait for the spawned thread to finish
    handle1.join().unwrap();
    handle2.join().unwrap();

}