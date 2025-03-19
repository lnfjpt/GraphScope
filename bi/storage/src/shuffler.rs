use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use log::info;

pub struct Message {
    pub content: Vec<u8>,
}

pub struct Shuffler {
    senders: Arc<Mutex<HashMap<usize, TcpStream>>>,
    receivers: Vec<Arc<Mutex<TcpStream>>>,

    handles: Vec<JoinHandle<()>>,

    pub worker_id: usize,
    pub worker_num: usize,
}

impl Shuffler {
    pub fn new(addrs: &Vec<String>, worker_id: usize, worker_num: usize) -> Self {
        assert_eq!(worker_num, addrs.len());
        let server_addr = addrs[worker_id].parse::<SocketAddr>().unwrap();
        let (senders_tx, senders_rx) = mpsc::channel();
        let (receivers_tx, receivers_rx) = mpsc::channel();
        let accept_handle = thread::spawn({
            let addr = server_addr.clone();
            let peers_num = worker_num - 1;
            move || {
                let listener = TcpListener::bind(&addr).unwrap();
                for _ in 0..peers_num {
                    let (socket, _) = listener.accept().unwrap();
                    receivers_tx.send(socket).unwrap();
                }
            }
        });
        let connect_handle = thread::spawn({
            let addrs = addrs.clone();
            move || {
                for i in 1..worker_num {
                    let target = (worker_id + i) % worker_num;
                    let addr = addrs[target].clone();
                    let mut attemps = 0;
                    loop {
                        match TcpStream::connect(&addr) {
                            Ok(stream) => {
                                senders_tx.send(stream).unwrap();
                                break;
                            },
                            Err(e) => {
                                attemps += 1;
                                info!("retry to connect to {}, attempts: {}", addr, attemps);
                                thread::sleep(Duration::from_millis(100));
                            },
                        }
                    }
                }
            }
        });

        let mut senders = HashMap::<usize, TcpStream>::new();

        for i in 1..worker_num {
            let stream = senders_rx.recv().unwrap();
            let target = (worker_id + i) % worker_num;
            senders.insert(target, stream);
        }

        let mut receivers = Vec::<Arc<Mutex<TcpStream>>>::new();

        while let Ok(stream) = receivers_rx.recv() {
            receivers.push(Arc::new(Mutex::new(stream)));
        }

        Self {
            senders: Arc::new(Mutex::new(senders)),
            receivers,
            handles: vec![],
            worker_id,
            worker_num,
        }
    }

    pub fn shuffle_start(&mut self) -> (mpsc::Sender<(usize, Message)>, mpsc::Receiver<Message>) {
        assert!(self.handles.is_empty());

        let (input_tx, input_rx) = mpsc::channel::<(usize, Message)>();
        let (output_tx, output_rx) = mpsc::channel::<Message>();

        let send_handle = thread::spawn(
        {
            let senders = self.senders.clone();
            move || {
                let mut send_time = 0_f64;
                let mut send_time2 = 0_f64;
                let mut send_time3 = 0_f64;
                let mut total_len = 0_usize;
                let mut total_send = 0_usize;
                let mut senders = senders.lock().unwrap();

                let st1 = Instant::now();
                while let Ok((target, message)) = input_rx.recv() {
                    let st = Instant::now();
                    if let Some(stream) = senders.get_mut(&target) {
                        let len = message.content.len() as u64;
                        if len > 0 {
                            total_send += 1;
                            total_len += len as usize;
                            let start = Instant::now();
                            stream.write_all(&len.to_be_bytes()).unwrap();
                            stream.write_all(&message.content).unwrap();
                            send_time += start.elapsed().as_secs_f64();
                        }
                    }
                    send_time2 += st.elapsed().as_secs_f64();
                }
                send_time3 += st1.elapsed().as_secs_f64();
                for (_, stream) in senders.iter_mut() {
                    let len = 0_u64;
                            let start = Instant::now();
                    stream.write_all(&len.to_be_bytes()).unwrap();
                            send_time += start.elapsed().as_secs_f64();
                }
                info!("sender elapsed: {:.2} s, {} / {} = {}, {}, {}", send_time, total_len, total_send, total_len as f64 / total_send as f64, send_time2, send_time3);
            }
        });
        self.handles.push(send_handle);

        let recv_thread_num = self.receivers.len();
        for recv_i in 0..recv_thread_num {
            let recv_handle = thread::spawn({
                let recver = self.receivers[recv_i].clone();
                let tx = output_tx.clone();
                move ||{
                let mut recver = recver.lock().unwrap();
                loop {
                    let mut len_buf = [0_u8; 8];
                    if recver.read_exact(&mut len_buf).is_err() {
                        break;
                    }
                    let len = u64::from_be_bytes(len_buf) as usize;
                    if len == 0 {
                        break;
                    }
                    let mut content = vec![0_u8; len];
                    if recver.read_exact(&mut content).is_err() {
                        break;
                    }
                    tx.send(Message { content }).unwrap();
                }
            }});
            self.handles.push(recv_handle);
        }

        (input_tx, output_rx)
    }

    pub fn shuffle_end(&mut self) {
        let handles = std::mem::replace(&mut self.handles, vec![]);
        for handle in handles.into_iter() {
            handle.join().unwrap();
        }
    }
}