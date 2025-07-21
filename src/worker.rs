use std::collections::HashMap;
use std::net::Shutdown;
use std::net::TcpListener;

use std::net::TcpStream;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SendError;
use std::sync::mpsc::TryRecvError;

use std::time::Duration;

use std::thread;
use std::thread::JoinHandle;

use std::sync::mpsc::Sender;

use polling::{Event, Events, Poller};

use crate::stream;
use crate::stream::RequestError;
use crate::stream::{ForwardStatus, Request, Stream};

const MAGIC_KEY: usize = 7;

pub struct WorkerWrapper {
    name: String,
    to_child: Sender<Message>,
    thread: JoinHandle<()>,
}

impl WorkerWrapper {
    pub fn new(name: &String, to_parent: Sender<Message>) -> Result<WorkerWrapper, std::io::Error> {
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

    pub fn send(&self, m: Message) -> Result<(), SendError<Message>> {
        log::debug!("Sending message {:?} to {}", m, self.name);
        self.to_child.send(m)
    }
}

#[derive(Debug)]
pub enum Message {
    //Ping,
    //Data( String),
    Attach(Stream, usize),
    //Stream(TcpStream),
    Exit,
}

pub struct Worker {
    name: String,
    poller: Poller,
    to_parent: Option<Sender<Message>>,
    from_parent: Option<Receiver<Message>>,
    streams: HashMap<usize, Stream>,
    id: usize,
    listener: Option<TcpListener>,
    pub workers: Vec<WorkerWrapper>,
    //ev,
}

pub fn event_handler(_: &stream::Stream) {
    log::info!("Wow I got a message !");
}

impl Worker {
    pub fn new(to_parent: Sender<Message>, from_parent: Receiver<Message>) -> Worker {
        let current_thread = thread::current();

        Worker {
            id: MAGIC_KEY + 1,
            name: current_thread.name().unwrap().to_string(),
            poller: Poller::new().unwrap(),
            streams: HashMap::new(),
            to_parent: Some(to_parent),
            from_parent: Some(from_parent),
            listener: None,
            workers: vec![],
        }
    }

    pub fn new_root(listener: TcpListener) -> Worker {
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
            streams: HashMap::new(),
            to_parent: None,
            from_parent: None,
            listener: Some(listener),
            workers: vec![],
        }
    }

    pub fn poll(&self, connection: &TcpStream, key: usize) {
        log::debug!("polling events on {}", connection.peer_addr().unwrap());
        unsafe {
            self.poller
                .add(connection, Event::readable(key))
                .expect("unable to add poller");
        }
    }

    /** attach a stream */
    pub fn attach(&mut self, mut new_stream: Stream, id: usize) {
        log::info!(
            "{}: {} attaching connection to id {}",
            self.name,
            new_stream.conn_a.peer_addr().unwrap(),
            id
        );
        //FIXME better error handling

        let current_stream = self.streams.get_mut(&id);
        match current_stream {
            Some(current) => {
                if current.conn_b.is_some() {
                    new_stream.answer(stream::Response::Busy);

                    new_stream.conn_a.shutdown(Shutdown::Both);
                    self.poller.delete(new_stream.conn_a);
                    return;
                } else {
                    //stream.connection.write(b"joined as B\npeered\n");
                    // Notify A side
                    //self.poll(&stream.connection, id + stream::MAX);

                    //FIXME : Dup
                    unsafe {
                        let added = self
                            .poller
                            .add(&new_stream.conn_a, Event::readable(id + stream::MAX));
                        match added {
                            Ok(_) => log::debug!("stream add in poll"),
                            Err(e) => {
                                log::debug!("!!! Error adding poll: {}", e);
                            }
                        }
                    }

                    new_stream.answer(stream::Response::Peered);
                    if current.mode() != stream::StreamMode::WaitingReconnect {
                        current.answer(stream::Response::Peered);
                    }
                    current.upgrade_peered(new_stream.conn_a);
                    log::debug!("attached as B");

                    // poll messages from A side, B side is already polled
                    unsafe {
                        self.poller
                            .add(&current.conn_a, Event::readable(id))
                            .expect("oops");
                    }
                }
            }
            None => {
                //stream.connection.write(b"joined as A\nwaiting peer\n");
                new_stream.answer(stream::Response::WaitingPeer);
                new_stream.upgrade_waiting_peer(id);
                new_stream.register_data_handler(|stream, buffer| { 
                    log::info!("Wow I got a message from {}: {}", stream.conn_a.peer_addr().unwrap(), String::from_utf8_lossy(buffer));
                });
                //XXX: do not poll right now, wait for B peer: self.poll(&new_stream.conn_a, id);
                self.streams.insert(id, new_stream);
                
            }
        }
    }

    pub fn detach(&mut self, key: usize, peer: stream::PeerConnection, close: bool) {
        let stream = self.streams.get_mut(&key);
        if stream.is_none() {
            log::debug!("stream not found for key {}", key);
            return;
        }
        let stream = stream.unwrap();

        match peer {
            stream::PeerConnection::A => {
                if close {
                    let _ = stream.conn_a.shutdown(Shutdown::Both);
                }
                log::debug!("un-polling peerA");
                self.poller
                    .delete(&stream.conn_a)
                    .expect("connection not found");
            }
            stream::PeerConnection::B => {
                log::debug!("un-polling peerB");
                let conn_b = stream.conn_b.as_ref().unwrap();
                if close {
                    let _ = conn_b.shutdown(Shutdown::Both);
                }
                self.poller.delete(&conn_b).expect("connection not found");
            }
        }

        if stream.conn_b.is_none() {
            // no peer, clean up
            log::debug!("no peer, clean up streams");
            self.streams.remove(&key).expect("oops");
            return;
        }

        match stream.strategy() {
            stream::DisconnectStrategy::Close => {
                log::debug!("strategy: close peers on disconnect");
                self.streams.remove(&key).expect("oops");
            }
            stream::DisconnectStrategy::Persist => {
                log::debug!("strategy: persist peers on disconnect");
                match peer {
                    stream::PeerConnection::A => {

                        // remove to poller, will be polled once upgraded
                        self.poller.delete( stream.conn_b.as_ref().unwrap()).expect("chiotte");

                        stream.downgrade_waiting_reconnect(true);

                    }
                    stream::PeerConnection::B => stream.downgrade_waiting_reconnect(false),
                }
            }
        }
    }

    pub fn join(self) {
        for worker in self.workers {
            let _ = worker.thread.join();
        }
    }

    pub fn notify_parent(&self, m: Message) {
        if self.to_parent.is_none() {
            return;
        }
        self.to_parent
            .as_ref()
            .unwrap()
            .send(m)
            .expect("error sending message to parent");
    }

    pub fn watch_events(&mut self) {
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
                    Ok((connection, remote_addr)) => {
                        // FIXME: rate limit...

                        log::info!("#accepting connection from {}", remote_addr);

                        //connection.set_nonblocking(true).expect("error ");

                        let mut stream = stream::Stream::new(
                            connection, 
                            stream::DisconnectStrategy::Close // default strategy
                        );

                        stream.answer(stream::Response::Welcome);
                        self.id += 1;
                        self.poll(&stream.conn_a, self.id);

                        /*
                        let worker = workers.get(round_robin).unwrap();
                        let _ = worker.send(Message::Attach(connection.try_clone()?));
                        round_robin = (round_robin + 1) % workers.len();
                        */

                        self.streams.insert(self.id, stream);
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

    pub fn close_all(&mut self) {
        // clean up connections
        log::debug!("Closing all connections");

        // close listener is defined
        if self.listener.is_some() {
            self.poller
                .delete(self.listener.as_mut().unwrap())
                .expect("err");
        }

        for stream in self.streams.values_mut() {
            if stream.mode() == stream::StreamMode::Init {
                stream.answer(stream::Response::Exit);
            }
            self.poller
                .delete(&stream.conn_a)
                .expect("connection not found");
        }
        self.streams.clear();
    }

    pub fn run(&mut self) {
        log::info!("{} thread started !", self.name);

        while self.read_message() {
            self.watch_events();
            self.check_timeouts();
        }

        self.close_all();

        log::debug!("Thread {} terminating", self.name);
    }

    /// read a message
    pub fn read_message(&mut self) -> bool {
        if self.from_parent.is_none() {
            return true;
        }
        // important: non blocking reader
        let received = self.from_parent.as_mut().unwrap().try_recv();
        match received {
            Err(e) => match e {
                TryRecvError::Empty => {}
                TryRecvError::Disconnected => {
                    log::debug!("reader disconnected");
                    return false;
                }
            },
            Ok(m) => match m {
                Message::Attach(stream, id) => {
                    log::info!(
                        "{}: {} attaching for id {}",
                        self.name,
                        stream.conn_a.peer_addr().unwrap(),
                        id
                    );
                    self.attach(stream, id);
                }
                Message::Exit => {
                    return false;
                }
            },
        }

        true
    }

    pub fn peer_id(id: usize) -> usize {
        if id > stream::MAX {
            id - stream::MAX
        } else {
            id + stream::MAX
        }
    }

    //FIXME: use timeout from config
    pub fn check_timeouts(&mut self) {
        let mut to_remove: Vec<usize> = vec![];

        for (id, stream) in self.streams.iter_mut() {
            if stream.timeout(true) {
                self.poller.delete(&stream.conn_a);
                to_remove.push(*id);
                continue;
            }
        }
        for id in to_remove {
            self.streams.remove(&id);
        }
    }

    pub fn read_command(&mut self, original_key: usize) {
        let real_key = if original_key > stream::MAX {
            original_key - stream::MAX
        } else {
            original_key
        };
        let reverse = original_key > stream::MAX;

        let stream = self
            .streams
            .get_mut(&real_key)
            .expect("unable to foind connection");

        let addr = stream.conn_a.peer_addr();
        let peer_addr = if addr.is_ok() {
            addr.unwrap().to_string()
        } else {
            addr.err().unwrap().to_string()
        };

        if self.listener.is_none() {
            // in a thread
            match stream.forward_stream(reverse) {
                ForwardStatus::ConnectionClosed(peer) => {
                    log::info!(
                        "{}: {} Connection {:?} is closed",
                        self.name,
                        peer_addr,
                        peer
                    );
                    self.detach(real_key, peer, false);
                    return;
                }
                ForwardStatus::Forwarded => (),
                ForwardStatus::NoPeer => {
                    log::info!("{}: {} no peer to this connection", self.name, peer_addr);
                }
            }
        } else {
            // in root
            log::debug!("reading header");
            match stream.read_init() {
                Ok(Request::Noop) => {}
                Err(RequestError::ConnectionClosed(peer)) => {
                    log::info!("{}: {} Connection ended", self.name, peer_addr);
                    self.detach(real_key, peer, false);
                    return;
                }
                Err(RequestError::BadSyntax) => {
                    log::info!("{}: {} Bad syntax", self.name, peer_addr);
                }
                Ok(Request::Join(id)) => {
                    log::info!("{}: {} join request with id {}", self.name, peer_addr, id);

                    log::debug!("detach connection from root thread");
                    let connection_copy = stream.conn_a.try_clone().expect("clone failed");
                    let strategy = stream.strategy();
                    self.detach(real_key, stream::PeerConnection::A, false);
                    let index = id % self.workers.len();

                    // FIXME: check clone...
                    log::debug!(
                        "choosing worker number {} (number of workers {})",
                        index,
                        self.workers.len()
                    );
                    self.workers
                        .get(index)
                        .unwrap()
                        .send(Message::Attach(stream::Stream::new(connection_copy, strategy), id))
                        .expect("error");

                    return;
                }
                Ok(Request::Exit) => {
                    log::info!("{}: {} global exit requested", self.name, peer_addr);
                    self.notify_parent(Message::Exit);
                    return;
                }
                Ok(Request::Strategy(strategy)) => {
                    log::info!("{}: {} switch strategy to {:?}", self.name, peer_addr, strategy);
                    stream.set_strategy( strategy);
                    stream.answer(stream::Response::Ok);
                }
                Ok(Request::Close) => {
                    log::info!("{}: {} closing connection", self.name, peer_addr);
                    //FIXME should identify if A or B
                    self.detach(real_key, stream::PeerConnection::A, true);
                    return;
                }
            }
        }

        if reverse {
            log::debug!("polling peer");
            self.poller
                .modify(
                    &stream.conn_b.as_ref().unwrap(),
                    Event::readable(original_key),
                )
                .expect("unable to update poller");
        } else {
            log::debug!("polling stream");
            self.poller
                .modify(&stream.conn_a, Event::readable(original_key))
                .expect("unable to update poller");
        }
    }
}
