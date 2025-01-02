use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::net::SocketAddr;
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::io::{BufReader, BufRead, self};
use std::io::Write;
use std::path::PathBuf;
use std::ptr::write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::fs::{self, OpenOptions};
use std::task::{Context, Poll};
use std::time::Duration;

use actix_web::cookie::time::Instant;
use shm_graph::graph_db::GraphDB;
use shm_graph::graph_modifier::*;

use futures::Stream;
use hyper::server::accept::Accept;
use hyper::server::conn::{AddrIncoming, AddrStream};
use pegasus::api::function::FnResult;
use pegasus::api::FromStream;
use pegasus::errors::{ErrorKind, JobExecError};
use pegasus::resource::DistributedParResourceMaps;
use pegasus::result::FromStreamExt;
use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_network::config::ServerAddr;
use pegasus_network::{get_msg_sender, get_recv_register};
use prost::Message;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;
use tokio_stream::iter;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::queries::register::QueryApi;
use crate::generated::common;
use crate::generated::procedure;
use crate::generated::protocol as pb;
use crate::queries::register::QueryRegister;

pub struct StandaloneServiceListener;

pub struct RpcSink {
    pub job_id: u64,
    had_error: Arc<AtomicBool>,
    peers: Arc<AtomicUsize>,
    tx: UnboundedSender<Result<pb::JobResponse, Status>>,
}

impl RpcSink {
    pub fn new(job_id: u64, tx: UnboundedSender<Result<pb::JobResponse, Status>>) -> Self {
        RpcSink {
            tx,
            had_error: Arc::new(AtomicBool::new(false)),
            peers: Arc::new(AtomicUsize::new(1)),
            job_id,
        }
    }
}

impl FromStream<Vec<u8>> for RpcSink {
    fn on_next(&mut self, resp: Vec<u8>) -> FnResult<()> {
        // todo: use bytes to alleviate copy & allocate cost;
        let res = pb::JobResponse { job_id: self.job_id, resp };
        self.tx.send(Ok(res)).ok();
        Ok(())
    }
}

impl Clone for RpcSink {
    fn clone(&self) -> Self {
        self.peers.fetch_add(1, Ordering::SeqCst);
        RpcSink {
            job_id: self.job_id,
            had_error: self.had_error.clone(),
            peers: self.peers.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl FromStreamExt<Vec<u8>> for RpcSink {
    fn on_error(&mut self, error: Box<dyn Error + Send>) {
        self.had_error.store(true, Ordering::SeqCst);
        let status = if let Some(e) = error.downcast_ref::<JobExecError>() {
            match e.kind {
                ErrorKind::WouldBlock(_) => {
                    Status::internal(format!("[Execution Error] WouldBlock: {}", error))
                }
                ErrorKind::Interrupted => {
                    Status::internal(format!("[Execution Error] Interrupted: {}", error))
                }
                ErrorKind::IOError => Status::internal(format!("[Execution Error] IOError: {}", error)),
                ErrorKind::IllegalScopeInput => {
                    Status::internal(format!("[Execution Error] IllegalScopeInput: {}", error))
                }
                ErrorKind::Canceled => {
                    Status::deadline_exceeded(format!("[Execution Error] Canceled: {}", error))
                }
                _ => Status::unknown(format!("[Execution Error]: {}", error)),
            }
        } else {
            Status::unknown(format!("[Unknown Error]: {}", error))
        };

        self.tx.send(Err(status)).ok();
    }
}

impl Drop for RpcSink {
    fn drop(&mut self) {
        let before_sub = self.peers.fetch_sub(1, Ordering::SeqCst);
        if before_sub == 1 {
            if !self.had_error.load(Ordering::SeqCst) {
                self.tx.send(Err(Status::ok("ok"))).ok();
            }
        }
    }
}

impl StandaloneServiceListener {
    fn on_rpc_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()> {
        info!("RPC server of server[{}] start on {}", server_id, addr);
        Ok(())
    }

    fn on_server_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()> {
        info!("compute server[{}] start on {}", server_id, addr);
        Ok(())
    }
}

pub struct RPCJobServer<S: pb::job_service_server::JobService> {
    service: S,
    rpc_config: RPCServerConfig,
}

impl<S: pb::job_service_server::JobService> RPCJobServer<S> {
    pub fn new(rpc_config: RPCServerConfig, service: S) -> Self {
        RPCJobServer { service, rpc_config }
    }

    pub async fn run(
        self, server_id: u64, mut listener: StandaloneServiceListener,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
        where {
        let RPCJobServer { service, mut rpc_config } = self;
        let mut builder = Server::builder();
        if let Some(limit) = rpc_config.rpc_concurrency_limit_per_connection {
            builder = builder.concurrency_limit_per_connection(limit);
        }

        if let Some(dur) = rpc_config.rpc_timeout_ms.take() {
            builder = builder.timeout(Duration::from_millis(dur));
        }

        if let Some(size) = rpc_config.rpc_initial_stream_window_size {
            builder = builder.initial_stream_window_size(Some(size));
        }

        if let Some(size) = rpc_config.rpc_initial_connection_window_size {
            builder = builder.initial_connection_window_size(Some(size));
        }

        if let Some(size) = rpc_config.rpc_max_concurrent_streams {
            builder = builder.max_concurrent_streams(Some(size));
        }

        if let Some(dur) = rpc_config.rpc_keep_alive_interval_ms.take() {
            builder = builder.http2_keepalive_interval(Some(Duration::from_millis(dur)));
        }

        if let Some(dur) = rpc_config.rpc_keep_alive_timeout_ms.take() {
            builder = builder.http2_keepalive_timeout(Some(Duration::from_millis(dur)));
        }

        let service = builder.add_service(pb::job_service_server::JobServiceServer::new(service));

        let rpc_host = rpc_config
            .rpc_host
            .clone()
            .unwrap_or("0.0.0.0".to_owned());
        let rpc_port = rpc_config.rpc_port.unwrap_or(0);
        let rpc_server_addr = ServerAddr::new(rpc_host, rpc_port);
        let addr = rpc_server_addr.to_socket_addr()?;
        let ka = rpc_config
            .tcp_keep_alive_ms
            .map(|d| Duration::from_millis(d));
        let incoming = TcpIncoming::new(addr, rpc_config.tcp_nodelay.unwrap_or(true), ka)?;
        info!("starting RPC job server on {} ...", incoming.inner.local_addr());
        listener.on_rpc_start(server_id, incoming.inner.local_addr())?;

        service.serve_with_incoming(incoming).await?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct RPCServerConfig {
    pub rpc_host: Option<String>,
    pub rpc_port: Option<u16>,
    pub rpc_concurrency_limit_per_connection: Option<usize>,
    pub rpc_timeout_ms: Option<u64>,
    pub rpc_initial_stream_window_size: Option<u32>,
    pub rpc_initial_connection_window_size: Option<u32>,
    pub rpc_max_concurrent_streams: Option<u32>,
    pub rpc_keep_alive_interval_ms: Option<u64>,
    pub rpc_keep_alive_timeout_ms: Option<u64>,
    pub tcp_keep_alive_ms: Option<u64>,
    pub tcp_nodelay: Option<bool>,
}

impl RPCServerConfig {
    pub fn new(rpc_host: Option<String>, rpc_port: Option<u16>) -> Self {
        RPCServerConfig {
            rpc_host,
            rpc_port,
            rpc_concurrency_limit_per_connection: None,
            rpc_timeout_ms: None,
            rpc_initial_stream_window_size: None,
            rpc_initial_connection_window_size: None,
            rpc_max_concurrent_streams: None,
            rpc_keep_alive_interval_ms: None,
            rpc_keep_alive_timeout_ms: None,
            tcp_keep_alive_ms: None,
            tcp_nodelay: None,
        }
    }

    pub fn parse(content: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(&content)
    }
}

pub async fn start_all(
    rpc_config: RPCServerConfig, server_config: Configuration, query_register: QueryRegister, pool_size: u32, workers: u32,
    servers: Vec<u64>, graph_db: Option<Arc<RwLock<GraphDB<usize, usize>>>>, graph_schema_path: PathBuf, partition_id: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_id = server_config.server_id();
    start_rpc_sever(server_id, rpc_config, query_register, pool_size, workers, &servers, graph_db, graph_schema_path, partition_id)
        .await?;
    Ok(())
}

pub async fn start_rpc_sever(
    server_id: u64, rpc_config: RPCServerConfig, query_register: QueryRegister, pool_size: u32, workers: u32,
    servers: &Vec<u64>, graph_db: Option<Arc<RwLock<GraphDB<usize, usize>>>>, graph_schema_path: PathBuf, partition_id: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut service = JobServiceImpl {
        query_register,
        workers,
        server_id,
        pool_size,
        servers: servers.clone(),
        report: true,
        graph_db,
        subprocess: Some(Arc::new(RwLock::new(VecDeque::new()))),
        graph_schema_path,
        partition_id,
    };
    service.start_subprocess();
    let server = RPCJobServer::new(rpc_config, service);
    server
        .run(server_id, StandaloneServiceListener {})
        .await?;
    Ok(())
}

static CODEGEN_TMP_DIR: &'static str = "CODEGEN_TMP_DIR";

#[allow(dead_code)]
pub struct JobServiceImpl {
    query_register: QueryRegister,
    workers: u32,
    server_id: u64,
    pool_size: u32,
    servers: Vec<u64>,
    report: bool,
    graph_db: Option<Arc<RwLock<GraphDB<usize, usize>>>>,
    subprocess: Option<Arc<RwLock<VecDeque<(u32, std::process::Child)>>>>,
    graph_schema_path: PathBuf,
    partition_id: usize,
}

impl JobServiceImpl {
    pub fn start_subprocess(&mut self) {
        if let Some(subprocess) = &self.subprocess {
            let mut subprocess_write = subprocess.write().expect("subprocess lock poisoned");
            println!("graph schema path is {}", self.graph_schema_path.to_str().unwrap());
            for i in 0..self.pool_size {
                let mut child = Command::new("/mnt/nas/subprocess/gie-codegen/GraphScope/interactive_engine/executor/server/target/release/run_query")
                    .stdin(Stdio::piped())
                    .env("RUST_LOG", "INFO")
                    .arg("-s")
                    .arg("/root/server.toml")
                    .arg("-q")
                    .arg("/mnt/nas/subprocess/queries.yaml")
                    .arg("-o")
                    .arg(format!("{}", i))
                    .arg("-i")
                    .arg(self.graph_schema_path.to_str().unwrap())
                    .arg("--partition_id")
                    .arg(self.partition_id.to_string())
                    .spawn()
                    .expect("Failed to execute command");
                subprocess_write.push_back((i, child));
            }
        }
    }
}

#[tonic::async_trait]
impl pb::job_service_server::JobService for JobServiceImpl {
    type SubmitStream = UnboundedReceiverStream<Result<pb::JobResponse, Status>>;

    async fn submit(&self, req: Request<pb::JobRequest>) -> Result<Response<Self::SubmitStream>, Status> {
        let pb::JobRequest { conf, source, plan, resource } = req.into_inner();
        let conf = conf.unwrap();
        let job_id = conf.job_id;
        if let Ok(query) = procedure::Query::decode(&*plan) {
            if let Some(query_name) = query.query_name {
                let query_name = match query_name.item {
                    Some(common::name_or_id::Item::Name(name)) => name,
                    _ => "unknown".to_string(),
                };
                let mut inputs = query_name.clone();
                let mut params = HashMap::<String, String>::new();
                for argument in query.arguments {
                    let name = argument.param_name;
                    let value = match argument.value.unwrap().item {
                        Some(common::value::Item::Str(value)) => value,
                        _ => panic!("Unsupport value type"),
                    };
                    params.insert(name, value);
                }
                if let Some((queries, query_type)) = self.query_register.get_new_query(&query_name) {
                    if query_type == "READ_WRITE" || query_type == "READ" {
                        let mut graph = self.graph_db.write().unwrap();
                        graph.apply_delete_neighbors();
                        drop(graph);
                    }
                    let start = Instant::now();
                    let resource_maps = DistributedParResourceMaps::default(
                        ServerConf::Partial(self.servers.clone()),
                        self.workers,
                    );
                    let mut conf = parse_conf_req(conf);
                    conf.reset_servers(ServerConf::Partial(self.servers.clone()));
                    let msg_sender_map = get_msg_sender();
                    let recv_register_map = get_recv_register();
                    for query in queries.into_iter() {
                        let graph = self.graph_db.read().unwrap();
                        let results = {
                            pegasus::run_with_resource_map(
                                conf.clone(),
                                Some(resource_maps.clone()),
                                || {
                                    query.Query(
                                        conf.clone(),
                                        &graph,
                                        params.clone(),
                                        None,
                                        msg_sender_map.clone(),
                                        recv_register_map.clone(),
                                    )
                                },
                            )
                                .expect("submit query failure")
                        };
                        let mut write_operations = vec![];
                        let mut bytes_result = vec![];
                        for result in results {
                            if let Ok((worker_id, alias_datas, write_ops, mut query_result)) = result {
                                if let Some(write_ops) = write_ops {
                                    for write_op in write_ops {
                                        write_operations.push(write_op);
                                    }
                                }
                                if let Some(mut query_result) = query_result {
                                    let len = query_result.len();
                                    bytes_result.append(&mut len.to_le_bytes().to_vec());
                                    bytes_result.append(&mut query_result);
                                }
                            }
                        }
                        println!("execute dataflow: {}", start.elapsed().as_seconds_f64());
                        drop(graph);
                        let mut graph = self.graph_db.write().unwrap();
                        apply_write_operations(&mut graph, write_operations, self.servers.len());


                        let start = Instant::now();
                        /*if let Some(subprocess) = &self.subprocess {
                    let mut subprocess_write = subprocess.write().expect("subprocess lock poisoned");
                    let (ref mut index, ref mut child) = subprocess_write.front_mut().expect("subprocess queue is empty");
                    {
                        let stdin = child.stdin.as_mut().expect("Failed to open stdin");
                        write!(stdin, "test");
                        stdin.flush()?;
                    }
                    //let _ = child.wait().expect("Child process wasn't running");
                }*/
                        let file_path = "/root/input";
                        let output_path = "/root/output";
                        let mut file = OpenOptions::new()
                            .write(true)
                            .truncate(true)
                            .create(true)
                            .open(file_path).expect("failed to open file");
                        write!(file, "{}", inputs).expect("Failed to write");
                        //write!(file, "test").expect("Failed to write");
                        drop(file);
                        let mut is_finished = false;
                        let mut bytes_result = vec![];
                        loop {
                            if let Ok(result) = fs::read_to_string(output_path) {
                                let result: Vec<String> = result.split('\n').map(|s| s.to_string()).collect();
                                if result.contains(&"Finished".to_string()) {
                                    println!("Find finished in final");
                                    for i in 0..result.len() - 1 {
                                        let query_result: Vec<u8> = result[i].as_bytes().to_vec();
                                        let len = query_result.len();
                                        bytes_result.append(&mut len.to_le_bytes().to_vec());
                                        bytes_result.append(&mut query_result.clone());
                                        println!("{}", result[i]);
                                    }
                                    is_finished = true;
                                }
                            }
                            if is_finished {
                                let mut file = OpenOptions::new()
                                    .write(true)
                                    .truncate(true)
                                    .open(output_path).expect("failed to open file");
                                break;
                            }
                        }

                        if !bytes_result.is_empty() {
                            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                            let response = pb::JobResponse { job_id, resp: bytes_result };
                            match tx.send(Ok(response)) {
                                Ok(_) => println!("Response sent successfully."),
                                Err(e) => eprintln!("Failed to send response: {}", e),
                            }
                            return Ok(Response::new(UnboundedReceiverStream::new(rx)));
                        }
                    };
                }
            }
        }
        let bytes_result = vec![];
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let response = pb::JobResponse { job_id, resp: bytes_result };
        match tx.send(Ok(response)) {
            Ok(_) => println!("Response sent successfully."),
            Err(e) => eprintln!("Failed to send response: {}", e),
        }
        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    async fn submit_call(
        &self, req: Request<pb::CallRequest>,
    ) -> Result<Response<pb::CallResponse>, Status> {
        let pb::CallRequest { query } = req.into_inner();
        let reply = pb::CallResponse { is_success: true, results: vec![], reason: "".to_string() };
        Ok(Response::new(reply))
    }
}

pub(crate) struct TcpIncoming {
    inner: AddrIncoming,
}

impl TcpIncoming {
    pub(crate) fn new(addr: SocketAddr, nodelay: bool, keepalive: Option<Duration>) -> hyper::Result<Self> {
        let mut inner = AddrIncoming::bind(&addr)?;
        inner.set_nodelay(nodelay);
        inner.set_keepalive(keepalive);
        Ok(TcpIncoming { inner })
    }
}

impl Stream for TcpIncoming {
    type Item = Result<AddrStream, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_accept(cx)
    }
}

fn parse_conf_req(req: pb::JobConfig) -> JobConf {
    let mut conf = JobConf::new(req.job_name);
    if req.job_id != 0 {
        conf.job_id = req.job_id;
    }

    if req.workers != 0 {
        conf.workers = req.workers;
    }

    if req.time_limit != 0 {
        conf.time_limit = req.time_limit;
    }

    if req.batch_size != 0 {
        conf.batch_size = req.batch_size;
    }

    if req.batch_capacity != 0 {
        conf.batch_capacity = req.batch_capacity;
    }

    if req.trace_enable {
        conf.trace_enable = true;
        conf.plan_print = true;
    }

    conf
}
