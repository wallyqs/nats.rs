use std::{
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::Duration,
};

fn ends_with_crlf(buf: &[u8]) -> bool {
    buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n'
}

fn read_line(stream: &mut TcpStream) -> Option<String> {
    let mut buf = vec![];
    while !ends_with_crlf(&buf) {
        let mut b = [0u8; 1];
        match stream.read(&mut b) {
            Ok(1) => buf.push(b[0]),
            _ => return None,
        }
    }
    buf.truncate(buf.len() - 2);
    String::from_utf8(buf).ok()
}

fn read_exact(stream: &mut TcpStream, len: usize) -> Option<Vec<u8>> {
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).ok()?;
    Some(buf)
}

fn info_line(max_payload: i64) -> String {
    format!(
        "INFO {{\
\"server_id\":\"stub\",\
\"server_name\":\"stub\",\
\"host\":\"127.0.0.1\",\
\"port\":0,\
\"version\":\"stub\",\
\"go\":\"stub\",\
\"max_payload\":{},\
\"proto\":0,\
\"client_id\":1\
}}\r\n",
        max_payload
    )
}

#[derive(Debug, Clone)]
enum ServerEvent {
    Pub {
        subject: String,
        #[allow(dead_code)]
        reply: Option<String>,
        payload: Vec<u8>,
    },
}

struct StubServer {
    port: u16,
    shutdown: Arc<AtomicBool>,
    events: Arc<Mutex<Vec<ServerEvent>>>,
    info_tx: mpsc::Sender<i64>,
    handle: Option<thread::JoinHandle<()>>,
}

impl StubServer {
    fn start(initial_max_payload: i64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let events: Arc<Mutex<Vec<ServerEvent>>> = Arc::new(Mutex::new(vec![]));
        let (info_tx, info_rx) = mpsc::channel::<i64>();

        let handle = {
            let shutdown = shutdown.clone();
            let events = events.clone();
            thread::spawn(move || {
                // Wait for the client to connect.
                let mut stream = loop {
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    match listener.accept() {
                        Ok((s, _)) => break s,
                        Err(_) => thread::sleep(Duration::from_millis(5)),
                    }
                };
                stream
                    .set_read_timeout(Some(Duration::from_millis(50)))
                    .unwrap();

                stream
                    .write_all(info_line(initial_max_payload).as_bytes())
                    .unwrap();

                loop {
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }

                    // Forward any queued INFO updates.
                    while let Ok(new_max) = info_rx.try_recv() {
                        stream.write_all(info_line(new_max).as_bytes()).unwrap();
                    }

                    let line = match read_line(&mut stream) {
                        Some(l) => l,
                        None => continue,
                    };

                    let mut parts = line.split(' ');
                    match parts.next().unwrap_or("") {
                        "CONNECT" => {}
                        "PING" => {
                            stream.write_all(b"PONG\r\n").unwrap();
                        }
                        "PONG" => {}
                        "SUB" | "UNSUB" => {}
                        "PUB" => {
                            // Args are either: subject len  OR  subject reply len
                            let a = parts.next().unwrap().to_string();
                            let b = parts.next().unwrap().to_string();
                            let c = parts.next();
                            let (subject, reply, len) = match c {
                                Some(c) => (a, Some(b), c.parse::<usize>().unwrap()),
                                None => (a, None, b.parse::<usize>().unwrap()),
                            };
                            let payload = read_exact(&mut stream, len).unwrap_or_default();
                            // consume trailing CRLF
                            let _ = read_exact(&mut stream, 2);
                            events.lock().unwrap().push(ServerEvent::Pub {
                                subject,
                                reply,
                                payload,
                            });
                        }
                        _ => {}
                    }
                }
            })
        };

        StubServer {
            port,
            shutdown,
            events,
            info_tx,
            handle: Some(handle),
        }
    }

    fn url(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    fn push_info(&self, max_payload: i64) {
        self.info_tx.send(max_payload).unwrap();
    }

    fn events(&self) -> Vec<ServerEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl Drop for StubServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn drain_pubs(server: &StubServer, timeout: Duration) -> Vec<ServerEvent> {
    let start = std::time::Instant::now();
    loop {
        let events = server.events();
        if !events.is_empty() || start.elapsed() >= timeout {
            return events;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn publish_within_max_payload_succeeds() {
    let server = StubServer::start(1024);
    let nc = nats::connect(&server.url()).unwrap();

    let payload = vec![b'x'; 100];
    nc.publish("foo", &payload).unwrap();
    nc.flush().ok();

    let events = drain_pubs(&server, Duration::from_secs(1));
    assert_eq!(events.len(), 1);
    match &events[0] {
        ServerEvent::Pub {
            subject, payload: p, ..
        } => {
            assert_eq!(subject, "foo");
            assert_eq!(p.len(), 100);
        }
    }
}

#[test]
fn publish_at_max_payload_succeeds() {
    let server = StubServer::start(1024);
    let nc = nats::connect(&server.url()).unwrap();

    let payload = vec![b'y'; 1024];
    nc.publish("foo", &payload).unwrap();
    nc.flush().ok();

    let events = drain_pubs(&server, Duration::from_secs(1));
    assert_eq!(events.len(), 1);
    match &events[0] {
        ServerEvent::Pub { payload: p, .. } => assert_eq!(p.len(), 1024),
    }
}

#[test]
fn publish_over_max_payload_returns_error_and_does_not_send() {
    let server = StubServer::start(1024);
    let nc = nats::connect(&server.url()).unwrap();

    let payload = vec![b'z'; 1025];
    let err = nc.publish("foo", &payload).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert!(
        err.to_string().contains("max_payload"),
        "unexpected error message: {}",
        err
    );

    // Give the stub a moment in case something did get written.
    thread::sleep(Duration::from_millis(100));
    assert!(
        server.events().is_empty(),
        "oversized PUB should not have been sent: {:?}",
        server.events()
    );

    // A subsequent in-range publish should still work on the same connection.
    nc.publish("foo", &vec![b'a'; 8]).unwrap();
    nc.flush().ok();
    let events = drain_pubs(&server, Duration::from_secs(1));
    assert_eq!(events.len(), 1);
}

#[test]
fn publish_request_over_max_payload_returns_error() {
    let server = StubServer::start(64);
    let nc = nats::connect(&server.url()).unwrap();

    let payload = vec![b'z'; 65];
    let err = nc.publish_request("foo", "reply", &payload).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);

    thread::sleep(Duration::from_millis(100));
    assert!(server.events().is_empty());
}

#[test]
fn max_payload_refreshes_after_async_info() {
    // Start generous, then shrink via a server-pushed INFO.
    let server = StubServer::start(8192);
    let nc = nats::connect(&server.url()).unwrap();

    // 100 bytes is fine under both limits — sanity check.
    nc.publish("foo", &vec![b'a'; 100]).unwrap();
    nc.flush().ok();
    let _ = drain_pubs(&server, Duration::from_secs(1));

    // Now shrink the limit to 16 bytes.
    server.push_info(16);
    // Give the inbound thread time to process the new INFO.
    thread::sleep(Duration::from_millis(200));

    let err = nc.publish("foo", &vec![b'b'; 32]).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);

    // 16 bytes should still go through.
    nc.publish("foo", &vec![b'c'; 16]).unwrap();
    nc.flush().ok();
}
