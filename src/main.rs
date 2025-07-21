//use std::hash::{DefaultHasher, Hash, Hasher};

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use std::thread;

use log;
use simple_logger;


use std::sync::mpsc;


mod config;
mod stream;
mod worker;
mod pair;

// TODO: prefer thread or workers ?


fn start_server(addr: &String, thread_number: usize) -> Result<(), Box<dyn std::error::Error>> {
    let socket = TcpListener::bind(addr)?;

    let mut root_worker = worker::Worker::new_root(socket);

    //let mut end = false;

    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&term))?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&term))?;

    let (sender, receiver) = mpsc::channel();

    log::debug!("number of thread to spanw {}", thread_number);
    for n in 1..=thread_number {
        log::debug!("iteration {}", n);
        let thread_name = format!("thread#{}", n).to_owned();
        let w = worker::WorkerWrapper::new(&thread_name, sender.clone());

        match w {
            Ok(thread) => root_worker.workers.push(thread),
            Err(e) => log::warn!("unable to create thread {}: {}", thread_name, e),
        }
    }

    log::info!("Starting listenning on {}...", addr);

    // The event loop.
    while !term.load(Ordering::Relaxed) {
        let received = receiver.try_recv();
        match received {
            Ok(m) => match m {
                worker::Message::Exit => {
                    log::debug!("main: received a request to exit, propagate it to workers");
                    //FIXME: prefer term
                    term.store(true, Ordering::Relaxed);
                }
                _ => (),
            },
            _ => (), // No message or Disconnected
        }

        root_worker.watch_events();
        root_worker.check_timeouts();
    }

    for worker in &root_worker.workers {
        let _ = worker.send(worker::Message::Exit);
    }

    root_worker.close_all();

    log::debug!("Waiting for thread terminaison");
    root_worker.join();
    
    Ok(())
}

fn main() -> std::io::Result<()> {
    simple_logger::SimpleLogger::new()
        .init()
        .expect("error init logs");

    let default_parallelism_approx = thread::available_parallelism().unwrap().get();

    log::debug!("Found {} core", default_parallelism_approx);

    let config = config::load().expect("unable to find config");

    match start_server(&config.listen, default_parallelism_approx) {
        Ok(_) => (),
        Err(e) => log::warn!("oops: {}", e),
    }

    Ok(())
}
