//use std::hash::{DefaultHasher, Hash, Hasher};

use std::collections::HashMap;
use std::env;
use std::error;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::Shutdown;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SendError;
use std::sync::mpsc::Sender;
use std::sync::mpsc::TryRecvError;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_yaml_ng::{self};

use log;
use simple_logger;

use polling::{Event, Events, Poller};

// TODO: prefer thread or workers ?
const MAGIC_KEY: usize = 7;
const MAX: usize = usize::MAX / 2 - 1;
const MIN: usize = 100;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Config {
    name: String,
    durability: i32,
    owner: Owner,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Owner {
    name: String,
    age: usize,
}

fn load_config(file: File) -> Result<Config, Box<dyn error::Error>> {
    let reader = BufReader::new(file);

    let deserialized_config: Config = serde_yaml_ng::from_reader(reader)?;

    Ok(deserialized_config)
}

#[derive(Debug)]
enum Message {
    //Ping,
    //Data( String),
    Attach(TcpStream, usize),
    //Stream(TcpStream),
    Exit,
}

enum Command {
    KeepaliveRequest,
    CloseRequest,
    ExitRequest,
    JoinRequest(usize),
    Unknown(String),
    ConnectionClosed,
    Noop,
    Forwarded,
    NoPeer,
}

impl Command {
    fn forward_stream(
        mut connection: &TcpStream,
        mut peer_connection: &Option<&TcpStream>,
    ) -> Command {

        // Do not read data till peer is not here
        // FIXME: should remove poller to avoid any unnecessary events
        if peer_connection.is_none() {
            return Command::NoPeer;
        }

        let mut request_buf: Vec<u8> = vec![];
        let mut buffer = [0; 128];

        //FIXME: loop ?
        let c = connection.read(&mut buffer).expect("read error");

        if c == 0 {
            return Command::ConnectionClosed;
        }
        request_buf.extend(buffer);

        //let data = String::from_utf8_unchecked(request_buf);
        //let data = data.trim_end_matches(['\r', '\n', '\0']);

        // forward data

        peer_connection.unwrap().write(&buffer);

        /*
        match connection.write(b"echo: ") {
            Ok(c) => match c {
                0 => Command::ConnectionClosed,
                _ => Command::Raw,
            },
            Err(e) => {
                log::warn!("error writing: {}", e);
                return Command::ConnectionClosed;
            }
        };
        match connection.write(&buffer) {
            Ok(c) => match c {
                0 => log::debug!("connection closed"),
                //FIXME: close connection
                _ => (),
            },
            Err(e) => log::warn!("error writing: {}", e),
        };
        */
        // log::debug!("{}: {} Received {} chars: |{}|", name, active_connection.peer_addr().unwrap(), c, data)

        Command::Forwarded
    }

    fn read_stream(mut connection: &TcpStream) -> Command {
        let mut request_buf: Vec<u8> = vec![];
        let mut buffer = [0; 128];

        //FIXME: loop ?
        let c = connection.read(&mut buffer).expect("read error");

        if c == 0 {
            return Command::ConnectionClosed;
        }
        request_buf.extend(buffer);

        unsafe {
            let data = String::from_utf8_unchecked(request_buf);
            let data = data.trim_end_matches(['\r', '\n', '\0']);
            // log::debug!("{}: {} Received {} chars: |{}|", name, active_connection.peer_addr().unwrap(), c, data);

            let words: Vec<_> = data.split_whitespace().collect();

            if words.len() == 0 {
                return Command::Noop;
            }

            let command = words[0];

            if command.eq("exit") {
                connection.write(b"ok\n").expect("error writing socket");
                return Command::ExitRequest;
            } else if command.eq("close") {
                connection
                    .write(b"closed!\n")
                    .expect("error writing socket");
                return Command::CloseRequest;
            } else if command.eq("join") {
                if words.len() != 2 {
                    connection
                        .write(b"bad usage\n")
                        .expect("error writing to socket");
                    return Command::Noop;
                } else {
                    let parsed: Result<usize, _> = words[1].parse();
                    match parsed {
                        Ok(id) => {
                            if id < MIN || id > MAX {
                                connection
                                    .write(format!("Err, min:{} max:{}\n", MIN, MAX).as_bytes())
                                    .expect("err writing");
                                return Command::Noop;
                            }
                            connection.write(b"ok\n").expect("err writing");
                            return Command::JoinRequest(id);
                        }
                        Err(_) => {
                            connection
                                .write(b"join parameter must be an unsiged intetger\n")
                                .expect("err writing");
                            return Command::Noop;
                        }
                    }
                }
            } else if command.eq("keepalive") {
                connection.write(b"ok\n").expect("error writing socket");
                return Command::KeepaliveRequest;
            } else {
                connection
                    .write(b"command unknown!\n")
                    .expect("error writing socket");
                return Command::Unknown(command.to_string());
            }
        }

        /*
        match active_connection.write(b"echo: ") {
            Ok(c) => match c {
                0 => log::info!("{} connection closed", name),
                //FIXME: close connection
                _ => (),
            }
            Err(e) => log::warn!("{} error writing: {}", name, e),
        }
        match active_connection.write(&buffer) {
            Ok(c) => match c {
                0 => log::debug!("{} connection closed", name),
                //FIXME: close connection
                _ => (),
            }
            Err(e) => log::warn!("{} error writing: {}", name, e),
        }
        // active_connection.flush(); // XXX necessity ?
        //continue reading events
        */
    }
}

struct WorkerWrapper {
    name: String,
    to_child: Sender<Message>,
    thread: JoinHandle<()>,
}

impl WorkerWrapper {
    fn new(name: &String, to_parent: Sender<Message>) -> Result<WorkerWrapper, std::io::Error> {
        let (to_child, from_parent) = mpsc::channel();
        let thread = thread::Builder::new()
            .name(name.clone())
            .spawn(move || Worker::new(to_parent, from_parent).run())
            .expect("unable to create thread");

        Ok(WorkerWrapper {
            name: name.clone(),
            to_child,
            thread,
            //stream: None,
        })
    }

    fn send(&self, m: Message) -> Result<(), SendError<Message>> {
        log::debug!("Sending message {:?} to {}", m, self.name);
        self.to_child.send(m)
    }
}

struct Worker {
    name: String,
    poller: Poller,
    to_parent: Option<Sender<Message>>,
    from_parent: Option<Receiver<Message>>,
    connections: HashMap<usize, TcpStream>,
    id: usize,
    listener: Option<TcpListener>,
    workers: Vec<WorkerWrapper>,
    //ev,
}

impl Worker {
    fn new(to_parent: Sender<Message>, from_parent: Receiver<Message>) -> Worker {
        let current_thread = thread::current();

        Worker {
            id: MAGIC_KEY + 1,
            name: current_thread.name().unwrap().to_string(),
            poller: Poller::new().unwrap(),
            connections: HashMap::new(),
            to_parent: Some(to_parent),
            from_parent: Some(from_parent),
            listener: None,
            workers: vec![],
        }
    }

    fn new_root(listener: TcpListener) -> Worker {
        let poller = Poller::new().unwrap();
        listener.set_nonblocking(true).expect("oops");
        unsafe {
            poller
                .add(&listener, Event::readable(MAGIC_KEY))
                .expect("oops");
        }

        Worker {
            id: MAGIC_KEY + 1,
            name: String::from("root"),
            poller: poller,
            connections: HashMap::new(),
            to_parent: None,
            from_parent: None,
            listener: Some(listener),
            workers: vec![],
        }
    }

    fn attach_auto(&mut self, connection: TcpStream) {
        self.attach(connection, self.id);
        self.id += 1;
    }

    /** attach a stream */
    fn attach(&mut self, connection: TcpStream, id: usize) {
        log::info!(
            "{}: {} attaching connection",
            self.name,
            connection.peer_addr().unwrap()
        );
        //FIXME better error handling

        //let key: = connection.as_fd();
        unsafe {
            self.poller
                .add(&connection, Event::readable(id))
                .expect("unable to add poller");
        }
        self.connections.insert(id, connection);
    }

    fn detach(&mut self, key: usize, close: bool) {
        let connection = self.connections.get(&key).unwrap();
        if close {
            let _ = connection.shutdown(Shutdown::Both);

            /* 
            let peer = Self::peer_id(key);

            match self.connections.get(&peer) {
                None => (),
                Some(peer_connection) => {
                    // Cannot write anymore
                    peer_connection.shutdown(Shutdown::Write);
                }
            }
            */
        }

        self.poller
            .delete(&connection)
            .expect("connection not found");
        self.connections.remove(&key).expect("oops");
    }

    fn notify_parent(&self, m: Message) {
        if self.to_parent.is_none() {
            return;
        }
        self.to_parent
            .as_ref()
            .unwrap()
            .send(m)
            .expect("error sending message to parent");
    }

    fn watch_events(&mut self) {
        let mut events = Events::new();
        events.clear();
        self.poller
            .wait(&mut events, Some(Duration::new(0, 200_000_000)))
            .expect("oops reading poller");

        if events.is_empty() {
            //println!("{} still waiting for events...", name);
            return;
        }

        for ev in events.iter() {
            if ev.key == MAGIC_KEY {
                //let socket = self.listener.as_mut().unwrap();
                match self.listener.as_ref().unwrap().accept() {
                    Err(e) => {
                        log::warn!("error while acceoting a connection: {}", e);
                        continue;
                    }
                    Ok((mut connection, remote_addr)) => {
                        log::info!("#accepting connection from {}", remote_addr);
                        connection.set_nonblocking(true).expect("error ");
                        connection
                            .write(b"Welcome to echo server (key words: keepalive, join, close, exit)\n")
                            .expect("oops");

                        self.attach_auto(connection);
                        /*
                        let worker = workers.get(round_robin).unwrap();
                        let _ = worker.send(Message::Attach(connection.try_clone()?));
                        round_robin = (round_robin + 1) % workers.len();
                        */
                    }
                }
                self.poller
                    .modify(self.listener.as_ref().unwrap(), Event::readable(MAGIC_KEY))
                    .expect("err");
            } else {
                self.read_command(ev.key);
            }
        }
    }

    fn close_all(&mut self) {
        // clean up connections
        log::debug!("Closing all connections");

        // close listener is defined
        if self.listener.is_some() {
            self.poller
                .delete(self.listener.as_mut().unwrap())
                .expect("err");
        }

        for connection in self.connections.values() {
            self.poller
                .delete(&connection)
                .expect("connection not found");
        }
        self.connections.clear();
    }

    fn run(&mut self) {
        log::info!("{} thread started !", self.name);

        loop {
            if self.read_message() {
                break;
            }

            self.watch_events();
        }

        self.close_all();

        log::debug!("Thread {} terminating", self.name);
    }

    /// read a message
    fn read_message(&mut self) -> bool {
        // important: non blocking reader
        if self.from_parent.is_none() {
            return false;
        }
        let received = self.from_parent.as_mut().unwrap().try_recv();
        match received {
            Err(e) => match e {
                TryRecvError::Empty => {
                    return false;
                }
                TryRecvError::Disconnected => {
                    log::debug!("reader disconnected");
                    return true;
                }
            },
            Ok(m) => {
                match m {
                    //Message::Ping => println!("{}: ping received", name),
                    //Message::Data(payload) => println!("{}: data {}", name, payload),
                    Message::Attach(mut connection, id) => {
                        if self.connections.get(&id).is_none() {
                            log::info!(
                                "{}: {} attached for id {}",
                                self.name,
                                connection.peer_addr().unwrap(),
                                id
                            );
                            self.attach(connection, id);
                            return false;
                        }

                        let peer = id + MAX;
                        log::info!(
                            "{}: {} already have a connection for id {}, checking peer {}",
                            self.name,
                            connection.peer_addr().unwrap(),
                            id,
                            peer
                        );

                        //FIXME: use method
                        if self.connections.get(&peer).is_some() {
                            log::info!(
                                "{}: {} peer of {} already exists",
                                self.name,
                                connection.peer_addr().unwrap(),
                                id,
                            );
                            let _ = connection.write(b"sorry!\n");
                            self.detach(id, true);
                            return true; // disconnected
                        }

                        log::info!(
                            "{}: {} attached for peer of {}",
                            self.name,
                            connection.peer_addr().unwrap(),
                            id
                        );
                        self.attach(connection, peer);
                    }
                    Message::Exit => {
                        for connection in self.connections.values_mut() {
                            let _ = connection.write(b"exiting!\n");
                            let _ = connection.shutdown(Shutdown::Both);
                            self.poller
                                .delete(&connection)
                                .expect("Error removing socker");
                        }
                        self.connections.clear();
                        return true;
                    }
                }
            }
        }

        false
    }

    fn peer_id(id: usize) -> usize {
        if id > MAX { id - MAX } else { id + MAX }
    }

    fn read_command(&mut self, key: usize) {
        let connection = self
            .connections
            .get(&key)
            .expect("unable to foind connection");

        let peer = Self::peer_id(key);

        let peer_connection = self.connections.get(&peer);
        let peer_addr = connection.peer_addr();
        let peer_addr = if peer_addr.is_ok() { 
            peer_addr.unwrap().to_string() 
        } else {
            peer_addr.err().unwrap().to_string()
        };

        if self.listener.is_none() {
            
            // in a thread
            match Command::forward_stream(connection, &peer_connection) {
                Command::ConnectionClosed => {
                    log::info!(
                        "{}: {} Connection is closed",
                        self.name,
                        peer_addr
                    );
                    /* 
                    if peer_connection.is_some() {
                        peer_connection.unwrap().shutdown(Shutdown::Write);
                    }
                    */
                    self.detach(key, false);
                    return;
                }
                Command::Forwarded => (),
                Command::NoPeer => {
                    log::info!(
                        "{}: {} no peer to this connection",
                        self.name,
                        peer_addr
                    );
                }
                _ => (), // these cases should not occurs
            }
        } else {
            // in root
            match Command::read_stream(connection) {
                Command::Noop => {
                    log::debug!("{}: {} noop", self.name, peer_addr);
                }
                Command::Unknown(command) => {
                    log::info!(
                        "{}: {} command {} unknown",
                        self.name,
                        peer_addr,
                        command
                    );
                }
                Command::ConnectionClosed => {
                    log::info!(
                        "{}: {} Connection ended",
                        self.name,
                        peer_addr
                    );
                    self.detach(key, false);
                    return;
                }
                Command::JoinRequest(id) => {
                    log::info!(
                        "{}: {} join request with id {}",
                        self.name,
                        peer_addr,
                        id
                    );

                    log::debug!("detach connection from root thread");
                    let connection_copy = connection.try_clone().expect("clone failed");
                    self.detach(key, false);
                    let index = id % self.workers.len();

                    log::debug!("choosing worker number {}", index);
                    self.workers
                        .get(index)
                        .unwrap()
                        .send(Message::Attach(connection_copy, id))
                        .expect("error");

                    return;
                }
                Command::ExitRequest => {
                    log::info!(
                        "{}: {} global exit requested",
                        self.name,
                        peer_addr
                    );
                    self.notify_parent(Message::Exit);
                }
                Command::CloseRequest => {
                    log::info!(
                        "{}: {} closing connection",
                        self.name,
                        peer_addr
                    );
                    self.detach(key, true);
                    return;
                }
                Command::KeepaliveRequest => {
                    log::info!(
                        "{}: {} keepalive request",
                        self.name,
                        peer_addr
                    );
                }
                Command::Forwarded => (),
                Command::NoPeer => (),
            }
        }

        self.poller
            .modify(&connection, Event::readable(key))
            .expect("unable to update poller");
    }
}

fn start_server(thread_number: usize) -> Result<(), Box<dyn std::error::Error>> {
    // Create a TCP listener
    // FIXME: use config
    let addr = "127.0.0.1:8000";
    let socket = TcpListener::bind(addr)?;

    //let mut workers: Vec<WorkerWrapper> = vec![];
    let mut round_robin: usize = 0;

    let mut root_worker = Worker::new_root(socket);

    //let mut end = false;

    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&term))?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&term))?;

    let (sender, receiver) = mpsc::channel();

    for n in 1..thread_number {
        let thread_name = format!("thread#{}", n).to_owned();
        let w = WorkerWrapper::new(&thread_name, sender.clone());

        match w {
            Ok(thread) => root_worker.workers.push(thread),
            Err(e) => log::warn!("unable to create thread {}: {}", thread_name, e),
        }
    }

    log::info!("Starting listenning on {}...", addr);

    // Create a poller and register interest in readability on the socket.
    //let poller = Poller::new()?;

    // The event loop.
    while !term.load(Ordering::Relaxed) {
        let received = receiver.try_recv();
        match received {
            Ok(m) => match m {
                Message::Exit => {
                    log::debug!("main: received a request to exit, propagate it to workers");
                    //FIXME: prefer term
                    term.store(true, Ordering::Relaxed);
                }
                _ => (),
            },
            _ => (), // No message or Disconnected
        }

        root_worker.watch_events();
    }

    for worker in &root_worker.workers {
        let _ = worker.send(Message::Exit);
    }

    log::debug!("Waiting for thread terminaison");
    //wait for thread terminaison
    for thread in root_worker.workers {
        let _ = thread.thread.join();
    }

    Ok(())
}

fn main() -> std::io::Result<()> {
    simple_logger::SimpleLogger::new()
        .init()
        .expect("error init logs");

    let default_parallelism_approx = thread::available_parallelism().unwrap().get();

    log::debug!("Found {} core", default_parallelism_approx);

    let mut paths: Vec<String> = vec![String::from("/etc")];

    match env::var("HOME") {
        Ok(p) => paths.push(p),
        _ => (),
    }

    match env::home_dir() {
        Some(p) => paths.push(p.display().to_string()),
        _ => (),
    }

    match env::current_dir() {
        Ok(p) => paths.push(p.display().to_string()),
        _ => (),
    }

    log::debug!(
        "code is running in: {}",
        env::current_dir().unwrap().display()
    );

    let configname = "plop.conf";
    let mut configfile: Option<File> = None;
    for p in paths.iter() {
        match File::open(format!("{}/{}", p, configname)) {
            Ok(f) => {
                log::debug!("config {} found in path {}", configname, p);
                configfile = Some(f);
                break;
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    log::debug!("{}/{} not found, trying next", p, configname)
                }
                _ => log::debug!("unable to open config {}/{}: {}", p, configname, e),
            },
        };
    }

    match configfile {
        Some(file) => match load_config(file) {
            Ok(conf) => log::debug!("Config loaded: {:?}", conf),
            Err(e) => log::warn!("error parsing config file: {}", e),
        },
        None => {
            log::warn!("unable to find config file {}", configname);
        }
    }

    match start_server(default_parallelism_approx) {
        Ok(_) => (),
        Err(e) => log::warn!("oops: {}", e),
    }

    log::debug!("End of program\n");

    Ok(())
}
