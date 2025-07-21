use std::fmt;
use std::net::Shutdown;
use std::time::Duration;
use std::{
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    time::Instant,
};

pub const MAX: usize = usize::MAX / 2 - 1;
pub const MIN: usize = 100;

const EOL: &'static str = "\r\n";

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub enum DisconnectStrategy {
    Close,
    Persist,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub enum StreamMode {
    Init,
    WaitingPeer,
    WaitingReconnect,
    Peered,
}

#[derive(Debug)]
pub struct Stream {
    pub conn_a: TcpStream,
    pub conn_b: Option<TcpStream>,
    pub last_activity: Instant, // beware it is on both side (A & B)
    mode: StreamMode,
    strategy: DisconnectStrategy,
    key: Option<usize>,
    //pub onJoin: Option<fn(&Self, key: usize)>,
    pub onData: Option<fn(&Self, &[u8])>,
    pub onClose: Option<fn(&Self)>,
}

pub enum Request {
    Close,
    Exit,
    Strategy(DisconnectStrategy),
    Join(usize),
    Noop,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub enum PeerConnection {
    A,
    B
}

pub enum RequestError {
    BadSyntax,
    ConnectionClosed(PeerConnection),
}

impl Request {
    pub fn from(tokens: Vec<&str>) -> Result<Request, RequestError> {
        if tokens.len() == 0 {
            return Ok(Request::Noop);
        }

        let command = tokens[0];
        match command {
            "close" => Ok(Request::Close),
            "exit" => Ok(Request::Exit), // FIXME: to remove
            "strategy" => {
                if tokens.len() != 2 {
                    return Err(RequestError::BadSyntax);
                }
                match tokens[1] {
                    "persist" => return Ok(Request::Strategy(DisconnectStrategy::Persist)),
                    "close" => return Ok(Request::Strategy(DisconnectStrategy::Close)),
                    _ => return Err(RequestError::BadSyntax),
                }
            }
            "join" => {
                if tokens.len() != 2 {
                    return Err(RequestError::BadSyntax);
                }
                let parsed: Result<usize, _> = tokens[1].parse();
                match parsed {
                    Ok(id) => {
                        if id < MIN || id > MAX {
                            return Err(RequestError::BadSyntax);
                        }
                        //connection.write(b"ok\n").expect("err writing");
                        return Ok(Request::Join(id));
                    }
                    Err(_) => Err(RequestError::BadSyntax),
                }
            }
            _ => Err(RequestError::BadSyntax),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Request::Close => "close",
            Request::Strategy(_) => "exit",
            Request::Join(_) => "join",
            _ => "",
        }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub enum Response {
    Welcome,
    Timeout,
    BadSyntax,
    Exit,
    Busy,
    Peered,
    WaitingPeer,
    Ok,
}

impl Response {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Response::Welcome => b"Welcome to echo server (key words: join, close, exit)",
            Response::BadSyntax => b"bad-syntax",
            Response::Exit => b"exit",
            Response::Timeout => b"timeout",
            Response::Busy => b"busy",
            Response::Peered => b"peered",
            Response::WaitingPeer => b"waiting-peer",
            Response::Ok => b"ok",
        }
    }
}

pub enum ForwardStatus {
    Forwarded,
    NoPeer,
    ConnectionClosed(PeerConnection),
}

impl Stream {
    pub fn new(connection: TcpStream, strategy: DisconnectStrategy) -> Stream {
        connection
            .set_nonblocking(true)
            .expect("error setting non blocking");
        Stream {
            conn_a: connection,
            conn_b: None,
            key: None,
            last_activity: Instant::now(),
            strategy,
            mode: StreamMode::Init,
            onData: None,
            onClose: None,
        }
    }

    pub fn register_data_handler(&mut self, handler: fn(&Self, &[u8])) {
        self.onData = Some(handler);
    }

    pub fn upgrade_waiting_peer(&mut self, key: usize) {
        self.mode = StreamMode::WaitingPeer;
        self.key = Some(key);
    }

    /*
    pub fn peer(&mut self, other: &mut Self) {
        self.peer = Some(other.connection.try_clone().expect("oops"));
        other.peer = Some(self.connection.try_clone().expect("ooops"));

        self.peer.as_mut().unwrap().set_nonblocking(true);
        other.peer.as_mut().unwrap().set_nonblocking(true);
    }
    */

    /// move conn_b to conn_a
    pub fn downgrade_waiting_reconnect(&mut self, swap: bool) {
        if swap {
            log::debug!("swapping A with B");
            if self.conn_b.is_none() {
                panic!("Cannot swap connection if conn_b is not defined");
            }
            self.conn_a = self.conn_b.as_ref().unwrap().try_clone().expect("oops");
        }

        self.conn_b = None;
        self.mode = StreamMode::WaitingReconnect;
    }

    pub fn upgrade_peered(&mut self, peer: TcpStream) {
        self.conn_b = Some(peer);
        self.mode = StreamMode::Peered;
    }

    pub fn mode(&self) -> StreamMode {
        self.mode
    }

    pub fn strategy(&self) -> DisconnectStrategy {
        self.strategy
    }

    pub fn set_strategy(&mut self, strategy: DisconnectStrategy) {
        self.strategy = strategy;
    }
    pub fn forward_stream(&mut self, reverse: bool) -> ForwardStatus {
        // Do not read data till peer is not here
        // FIXME: should remove poller to avoid any unnecessary events, letting kernel to keep messages
        // FIXME: will create an infinite loop
        if self.conn_b.is_none() {
            return ForwardStatus::NoPeer;
        }

        let mut request_buf: Vec<u8> = vec![];
        let mut buffer = [0; 512];

        //FIXME: loop ?

        //let mut reader = if reverse { self.peer.as_mut().unwrap() } else { &self.connection};
        //let mut writer = if reverse {  &self.connection } else { self.peer.as_mut().unwrap() };

        if reverse {
            let c = self.conn_b.as_mut().unwrap().read(&mut buffer);

            match c {
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    log::error!("oops data not ready (WouldBlock)");
                    //return ForwardStatus::Forwarded;
                    panic!("should not occurs...");
                }
                Err(e) => {
                    log::error!("oops data not ready {}", e);
                    return ForwardStatus::Forwarded;
                }
                Ok(c) => {
                    if c == 0 {
                        return ForwardStatus::ConnectionClosed(PeerConnection::B);
                    }
                    request_buf.extend(buffer);

                    self.last_activity = Instant::now();

                    self.conn_a.write(&buffer);
                }
            }
        } else {
            let c = self.conn_a.read(&mut buffer).expect("read error");

            if c == 0 {
                return ForwardStatus::ConnectionClosed(PeerConnection::A);
            }
            request_buf.extend(buffer);

            self.last_activity = Instant::now();

            self.conn_b.as_mut().unwrap().write(&buffer);

            if self.onData.is_some() {
                (self.onData.unwrap())(&self, &buffer);
            }
        }

        //peer_stream.unwrap().connection.write(&buffer);

        // log::debug!("{}: {} Received {} chars: |{}|", name, active_connection.peer_addr().unwrap(), c, data)

        ForwardStatus::Forwarded
    }

    pub fn answer(&mut self, response: Response) {
        self.conn_a
            .write(response.as_bytes())
            .expect("error writing socket");
        self.conn_a
            .write(EOL.as_bytes())
            .expect("error writing socket");
    }

    pub fn read_init(&mut self) -> Result<Request, RequestError> {
        let mut request_buf: Vec<u8> = vec![];
        let mut buffer = [0; 512];

        //FIXME: loop ?
        let c = self.conn_a.read(&mut buffer).expect("read error");

        if c == 0 {
            return Err(RequestError::ConnectionClosed(PeerConnection::A));
        }
        request_buf.extend(buffer);

        self.last_activity = Instant::now();

        unsafe {
            let data = String::from_utf8_unchecked(request_buf);
            let data = data.trim_end_matches(['\r', '\n', '\0']);
            // log::debug!("{}: {} Received {} chars: |{}|", name, active_connection.peer_addr().unwrap(), c, data);

            let tokens: Vec<_> = data.split_whitespace().collect();

            match Request::from(tokens) {
                Err(RequestError::BadSyntax) => {
                    self.answer(Response::BadSyntax);

                    // FIXME
                    return Err(RequestError::BadSyntax);
                }
                Err(e) => {
                    return Err(e);
                }
                Ok(Request::Noop) => {
                    return Ok(Request::Noop);
                }
                Ok(request) => {
                    log::debug!("got a request: {}", request);
                    //self.connection
                    //    .write(b"ok\n")
                    //    .expect("error writing socket");
                    return Ok(request);
                }
            }
        }
    }

    pub fn timeout(&mut self, auto_close: bool) -> bool {
        //FIXME: use config
        let timeout = match self.mode {
            StreamMode::Init => Some(Duration::new(10, 0)),
            StreamMode::WaitingPeer => Some(Duration::new(30, 0)),
            StreamMode::Peered => Some(Duration::new(30, 0)),
            StreamMode::WaitingReconnect => Some(Duration::new(30, 0)),
        };

        if timeout.is_none() {
            return false;
        }

        if Instant::now().duration_since(self.last_activity) > timeout.unwrap() {
            //FIXME: got 2 connections when peered
            log::info!(
                "timeout on connection key {:?} - {} in phase {:?}",
                self.key,
                self.conn_a.peer_addr().unwrap(),
                self.mode
            );
            if auto_close {
                if self.mode != StreamMode::Peered {
                    self.answer(Response::Timeout);
                }
                self.conn_a.shutdown(Shutdown::Both);
            }

            // FIXME: what about monitoring peer ?

            return true;
        }
        false
    }
}
