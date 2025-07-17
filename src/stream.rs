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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub enum StreamMode {
    Init,
    WaitingPeer,
    Peered,
}

#[derive(Debug)]
pub struct Stream {
    pub connection: TcpStream,
    pub peer: Option<TcpStream>,
    pub last_activity: Instant, // beware it is on both side (A & B)
    mode: StreamMode,
    key: Option<usize>,
}

pub enum Request {
    Keepalive,
    Close,
    Exit,
    Join(usize),
    Noop,
}

pub enum RequestError {
    BadSyntax,
    ConnectionClosed,
}

impl Request {
    pub fn from(tokens: Vec<&str>) -> Result<Request, RequestError> {
        if tokens.len() == 0 {
            return Ok(Request::Noop);
        }

        let command = tokens[0];
        match command {
            "keepalive" => Ok(Request::Keepalive),
            "close" => Ok(Request::Close),
            "exit" => Ok(Request::Exit),
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
            Request::Keepalive => "keepalive",
            Request::Close => "close",
            Request::Exit => "exit",
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
    Closed,
    Error(String),
    Joined(String),
    Timeout,
    Exit,
}

impl Response {
    fn as_string(&self) -> String {
        match self {
            Response::Closed => String::from("Closed"),
            Response::Exit => String::from("Exit"),
            Response::Joined(side) => format!("Joined {}", side),
            Response::Error(reason) => format!("Error: {}", reason),
            Response::Timeout => String::from("Timeout"),
        }
    }
}

pub enum ForwardStatus {
    Forwarded,
    NoPeer,
    ConnectionClosed,
}

impl Stream {
    pub fn new(connection: TcpStream) -> Stream {
        Stream {
            connection: connection,
            peer: None,
            key: None,
            last_activity: Instant::now(),
            mode: StreamMode::Init,
        }
    }

    pub fn upgrade_waiting_peer( &mut self, key: usize) {
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

    pub fn upgrade_peered(&mut self, peer: TcpStream) {
        self.peer = Some(peer);
        self.mode = StreamMode::Peered;
    }

    pub fn mode(&self) -> StreamMode {
        self.mode
    }

    pub fn forward_stream(&mut self, reverse: bool) -> ForwardStatus {
        // Do not read data till peer is not here
        // FIXME: should remove poller to avoid any unnecessary events
        // FIXME: will create an infinite loop
        if self.peer.is_none() {
            return ForwardStatus::NoPeer;
        }

        let mut request_buf: Vec<u8> = vec![];
        let mut buffer = [0; 512];

        //FIXME: loop ?

        //let mut reader = if reverse { self.peer.as_mut().unwrap() } else { &self.connection};
        //let mut writer = if reverse {  &self.connection } else { self.peer.as_mut().unwrap() };

        if reverse {
            let c = self.peer.as_mut().unwrap().read(&mut buffer);

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
                        return ForwardStatus::ConnectionClosed;
                    }
                    request_buf.extend(buffer);

                    self.last_activity = Instant::now();

                    self.connection.write(&buffer);
                }
            }
        } else {
            let c = self.connection.read(&mut buffer).expect("read error");

            if c == 0 {
                return ForwardStatus::ConnectionClosed;
            }
            request_buf.extend(buffer);

            self.last_activity = Instant::now();

            self.peer.as_mut().unwrap().write(&buffer);
        }

        //peer_stream.unwrap().connection.write(&buffer);

        // log::debug!("{}: {} Received {} chars: |{}|", name, active_connection.peer_addr().unwrap(), c, data)

        ForwardStatus::Forwarded
    }

    pub fn read_init(&mut self) -> Result<Request, RequestError> {
        let mut request_buf: Vec<u8> = vec![];
        let mut buffer = [0; 512];

        //FIXME: loop ?
        let c = self.connection.read(&mut buffer).expect("read error");

        if c == 0 {
            return Err(RequestError::ConnectionClosed);
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
                    self.connection
                        .write(b"bad syntax\n")
                        .expect("error writing socket");

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
                    self.connection
                        .write(b"ok\n")
                        .expect("error writing socket");
                    return Ok(request);
                }
            }
        }
    }

    pub fn timeout(&mut self, auto_close: bool) -> bool {
        //FIXME: use config
        let timeout = match self.mode {
            StreamMode::Init        => Some(Duration::new(10, 0)),
            StreamMode::WaitingPeer => Some(Duration::new(30, 0)),
            StreamMode::Peered      => Some(Duration::new(30, 0)),
        };

        if timeout.is_none() {
            return false;
        }

        if Instant::now().duration_since(self.last_activity) > timeout.unwrap() {
            //FIXME: got 2 connections when peered
            log::info!(
                "timeout on connection key {:?} - {} in phase {:?}",
                self.key,
                self.connection.peer_addr().unwrap(),
                self.mode
            );
            if auto_close {
                if self.mode != StreamMode::Peered {
                    self.connection.write(b"timeout\n");
                }
                self.connection.shutdown(Shutdown::Both);
            }

            // FIXME: what about monitoring peer ?

            return true;
        }
        false
    }
}
