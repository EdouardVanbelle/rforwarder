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
    pub fn attach(&mut self, mut stream: Stream, id: usize) {
        log::info!(
            "{}: {} attaching connection to id {}",
            self.name,
            stream.connection.peer_addr().unwrap(),
            id
        );
        //FIXME better error handling

        let current_stream = self.streams.get_mut(&id);
        match current_stream {
            Some(current) => {
                if current.peer.is_some() {
                    stream.answer(stream::Response::Busy);

                    stream.connection.shutdown(Shutdown::Both);
                    self.poller.delete(stream.connection);
                    return;
                } else {
                    stream.answer(stream::Response::Peered);
                    //stream.connection.write(b"joined as B\npeered\n");
                    // Notify A side
                    current.answer(stream::Response::Peered);
                    
                    //self.poll(&stream.connection, id + stream::MAX);
                     
                    //FIXME : Dup
                    unsafe {
                        self.poller
                            .add(&stream.connection, Event::readable(id + stream::MAX))
                            .expect("unable to add poller");
                    }   
                    
                    current.upgrade_peered( stream.connection);
                }
            }
            None => {
                //stream.connection.write(b"joined as A\nwaiting peer\n");
                stream.answer(stream::Response::WaitingPeer);
                stream.upgrade_waiting_peer(id);
                self.poll(&stream.connection, id);
                self.streams.insert(id, stream);
            }
        }
    }

    pub fn detach(&mut self, key: usize, close: bool) {
        let stream = self.streams.get(&key);
        if stream.is_none() {
            log::debug!("stream not found for key {}", key);
            return;
        }
        let stream = stream.unwrap();
        if close {
            let _ = stream.connection.shutdown(Shutdown::Both);

            // FIXME close peer ?
            //let peer = Self::peer_id(key);
            //self.detach(peer, true);
        }

        self.poller
            .delete(&stream.connection)
            .expect("connection not found");

        if stream.peer.is_some() {
            //2 options: close the B side too or move B side to A side
            let peer = stream.peer.as_ref().unwrap();
            if close {
                let _ = peer.shutdown(Shutdown::Both);
            }
            self.poller
                .delete(&peer)
                .expect("connection not found");
            // FIXME: remove key + MAX ?
        }
        self.streams.remove(&key).expect("oops");

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
                    Ok((mut connection, remote_addr)) => {
                        // FIXME: rate limit...

                        log::info!("#accepting connection from {}", remote_addr);

                        //connection.set_nonblocking(true).expect("error ");

                        let mut stream = stream::Stream::new(connection);

                        stream.answer(stream::Response::Welcome);
                        self.id += 1;
                        self.poll(&stream.connection, self.id);
                        
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
                .delete(&stream.connection)
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
                        stream.connection.peer_addr().unwrap(),
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
                self.poller.delete(&stream.connection);
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

        let addr = stream.connection.peer_addr();
        let peer_addr = if addr.is_ok() {
            addr.unwrap().to_string()
        } else {
            addr.err().unwrap().to_string()
        };

        if self.listener.is_none() {
            // in a thread
            match stream.forward_stream(reverse) {
                ForwardStatus::ConnectionClosed => {
                    log::info!("{}: {} Connection is closed", self.name, peer_addr);
                    self.detach(real_key, false);
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
                Err(RequestError::ConnectionClosed) => {
                    log::info!("{}: {} Connection ended", self.name, peer_addr);
                    self.detach(real_key, false);
                    return;
                }
                Err(RequestError::BadSyntax) => {
                    log::info!("{}: {} Bad syntax", self.name, peer_addr);
                }
                Ok(Request::Join(id)) => {
                    log::info!("{}: {} join request with id {}", self.name, peer_addr, id);

                    log::debug!("detach connection from root thread");
                    let connection_copy = stream.connection.try_clone().expect("clone failed");
                    self.detach(real_key, false);
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
                        .send(Message::Attach(stream::Stream::new(connection_copy), id))
                        .expect("error");

                    return;
                }
                Ok(Request::Exit) => {
                    log::info!("{}: {} global exit requested", self.name, peer_addr);
                    self.notify_parent(Message::Exit);
                    return;
                }
                Ok(Request::Close) => {
                    log::info!("{}: {} closing connection", self.name, peer_addr);
                    self.detach(real_key, true);
                    return;
                }
            }
        }

        if reverse {
            log::debug!("polling peer");
            self.poller
                .modify(&stream.peer.as_ref().unwrap(), Event::readable(original_key))
                .expect("unable to update poller");
        } else {
            log::debug!("polling stream");
            self.poller
                .modify(&stream.connection, Event::readable(original_key))
                .expect("unable to update poller");
        }
    }
}
