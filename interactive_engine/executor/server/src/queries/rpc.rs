use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use hyper::server::accept::Accept;
use hyper::server::conn::{AddrIncoming, AddrStream};
use regex::Regex;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use bmcsr::graph_db::GraphDB;
use bmcsr::graph_modifier::{DeleteGenerator, GraphModifier};
use bmcsr::schema::InputSchema;
use bmcsr::traverse::traverse;
use graph_index::GraphIndex;
use pegasus::api::function::FnResult;
use pegasus::api::FromStream;
use pegasus::result::{FromStreamExt, ResultSink};
use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_network::config::ServerAddr;

use crate::generated::protocol as pb;
use crate::queries::register::QueryRegister;

pub struct StandaloneServiceListener;

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

pub struct RpcSink {
    pub job_id: u64,
    had_error: Arc<AtomicBool>,
    peers: Arc<AtomicUsize>,
    tx: UnboundedSender<Result<pb::BiJobResponse, Status>>,
}

impl RpcSink {
    pub fn new(job_id: u64, tx: UnboundedSender<Result<pb::BiJobResponse, Status>>) -> Self {
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
        let res = pb::BiJobResponse { job_index: self.job_id, resp };
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
        let status = Status::unknown(format!("execution_error: {}", error));
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

pub struct RPCJobServer<S: pb::bi_job_service_server::BiJobService> {
    service: S,
    rpc_config: RPCServerConfig,
}

impl<S: pb::bi_job_service_server::BiJobService> RPCJobServer<S> {
    pub fn new(rpc_config: RPCServerConfig, service: S) -> Self {
        RPCJobServer { service, rpc_config }
    }

    pub async fn run(
        self, server_id: u64, mut listener: StandaloneServiceListener,
    ) -> Result<(), Box<dyn std::error::Error>>
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

        let service = builder.add_service(pb::bi_job_service_server::BiJobServiceServer::new(service));

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
    rpc_config: RPCServerConfig, server_config: Configuration, query_register: QueryRegister, workers: u32,
    servers: &Vec<u64>, graph_db: Arc<RwLock<GraphDB<usize, usize>>>, graph_index: Arc<RwLock<GraphIndex>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let server_id = server_config.server_id();
    start_rpc_sever(server_id, rpc_config, query_register, workers, servers, graph_db, graph_index).await?;
    Ok(())
}

pub async fn start_rpc_sever(
    server_id: u64, rpc_config: RPCServerConfig, query_register: QueryRegister, workers: u32,
    servers: &Vec<u64>, graph_db: Arc<RwLock<GraphDB<usize, usize>>>, graph_index: Arc<RwLock<GraphIndex>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = JobServiceImpl {
        query_register,
        workers,
        servers: servers.clone(),
        report: true,
        graph_db,
        graph_index,
    };
    let server = RPCJobServer::new(rpc_config, service);
    server
        .run(server_id, StandaloneServiceListener {})
        .await?;
    Ok(())
}

#[allow(dead_code)]
pub struct JobServiceImpl {
    query_register: QueryRegister,
    workers: u32,
    servers: Vec<u64>,
    report: bool,

    graph_db: Arc<RwLock<GraphDB<usize, usize>>>,
    graph_index: Arc<RwLock<GraphIndex>>,
}

#[tonic::async_trait]
impl pb::bi_job_service_server::BiJobService for JobServiceImpl {
    type SubmitStream = UnboundedReceiverStream<Result<pb::BiJobResponse, Status>>;

    async fn submit(&self, req: Request<pb::BiJobRequest>) -> Result<Response<Self::SubmitStream>, Status> {
        debug!("accept new request from {:?};", req.remote_addr());
        let pb::BiJobRequest { job_name, arguments } = req.into_inner();

        let mut conf = JobConf::new(job_name.clone().to_owned());
        conf.set_workers(self.workers);
        conf.reset_servers(ServerConf::Partial(self.servers.clone()));
        let job_id = conf.job_id;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let rpc_sink = RpcSink::new(job_id, tx);
        let sink = ResultSink::<Vec<u8>>::with(rpc_sink);

        let mut input_params = HashMap::new();
        for i in arguments {
            input_params.insert(i.param_name, i.value);
        }

        let graph = self.graph_db.read().unwrap();
        let graph_index = self.graph_index.read().unwrap();

        if let Some(libc) = self.query_register.get_query(&job_name) {
            let conf_temp = conf.clone();
            if let Err(e) = pegasus::run_opt(conf, sink, |worker| {
                worker.dataflow(libc.Query(conf_temp.clone(), &graph, &graph_index, input_params.clone()))
            }) {
                error!("submit job {} failure: {:?}", job_id, e);
                Err(Status::unknown(format!("submit job error {}", e)))
            } else {
                Ok(Response::new(UnboundedReceiverStream::new(rx)))
            }
        } else {
            Err(Status::unknown(format!("query not found, job name {}", job_name)))
        }
    }

    async fn submit_batch_insert(
        &self, req: Request<pb::BatchUpdateRequest>,
    ) -> Result<Response<pb::BatchUpdateResponse>, Status> {
        let pb::BatchUpdateRequest { data_root, request_json } = req.into_inner();
        let data_root_path = PathBuf::from(data_root);

        let mut graph_modifier = GraphModifier::new(&data_root_path);

        graph_modifier.skip_header();
        graph_modifier.parallel(self.workers);
        let mut graph = self.graph_db.write().unwrap();
        let insert_schema = InputSchema::from_string(request_json, &graph.graph_schema).unwrap();
        graph_modifier
            .insert(&mut graph, &insert_schema)
            .unwrap();

        let reply = pb::BatchUpdateResponse { is_success: true };
        Ok(Response::new(reply))
    }

    async fn submit_batch_delete(
        &self, req: Request<pb::BatchUpdateRequest>,
    ) -> Result<Response<pb::BatchUpdateResponse>, Status> {
        let pb::BatchUpdateRequest { data_root, request_json } = req.into_inner();
        let data_root_path = PathBuf::from(data_root);

        let mut graph_modifier = GraphModifier::new(&data_root_path);

        graph_modifier.skip_header();
        graph_modifier.parallel(self.workers);
        let mut graph = self.graph_db.write().unwrap();
        let delete_schema = InputSchema::from_string(request_json, &graph.graph_schema).unwrap();
        graph_modifier
            .delete(&mut graph, &delete_schema)
            .unwrap();

        let reply = pb::BatchUpdateResponse { is_success: true };
        Ok(Response::new(reply))
    }

    async fn submit_delete_generation(
        &self, req: Request<pb::DeleteGenerationRequest>,
    ) -> Result<Response<pb::DeleteGenerationResponse>, Status> {
        let pb::DeleteGenerationRequest { raw_data_root, batch_id } = req.into_inner();

        let graph = self.graph_db.read().unwrap();
        let raw_data_root = PathBuf::from(raw_data_root);
        let mut delete_generator = DeleteGenerator::new(&raw_data_root);
        delete_generator.skip_header();
        delete_generator.generate(&graph, batch_id.as_str());

        let reply = pb::DeleteGenerationResponse { is_success: true };
        Ok(Response::new(reply))
    }

    async fn submit_precompute(
        &self, req: Request<pb::PrecomputeRequest>,
    ) -> Result<Response<pb::PrecomputeResponse>, Status> {
        let graph = self.graph_db.read().unwrap();
        let mut graph_index = self.graph_index.write().unwrap();
        self.query_register
            .run_precomputes(&graph, &mut graph_index, self.workers);

        let reply = pb::PrecomputeResponse { is_success: true };
        Ok(Response::new(reply))
    }

    async fn submit_traverse(
        &self, req: Request<pb::TraverseRequest>,
    ) -> Result<Response<pb::TraverseResponse>, Status> {
        let pb::TraverseRequest { output_dir } = req.into_inner();
        std::fs::create_dir_all(&output_dir).unwrap();
        let graph = self.graph_db.read().unwrap();
        traverse(&graph, &output_dir);

        let reply = pb::TraverseResponse { is_success: true };
        Ok(Response::new(reply))
    }

    async fn submit_call(
        &self, req: Request<pb::CallRequest>,
    ) -> Result<Response<pb::CallResponse>, Status> {
        let pb::CallRequest { query } = req.into_inner();
        let function_name_re = Regex::new(r"^CALL (.*)\(([\s\S]*?)\)$").unwrap();
        let (function_name, parameters) = if function_name_re.is_match(&query) {
            let capture = function_name_re
                .captures(&query)
                .expect("Capture function_name error");
            println!("function_name: {}, parameters: {}", capture[1].to_string(), capture[2].to_string());
            (capture[1].to_string(), capture[2].to_string())
        } else {
            let reply = pb::CallResponse { is_success: true, results: vec![], reason: "".to_string() };
            return Ok(Response::new(reply));
        };
        match function_name.as_str() {
            "gs.flex.custom.asProcedure" => {
                let parameters_re = Regex::new(
                    r"^\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*(\[(?:\['[^']*'(?:,\s*'[^']*')*\]\
                           (?:,\s*)?)*\])\s*,\s*(\[(?:\['[^']*'(?:,\s*'[^']*')*\](?:,\s*)?)*\])\s*,\s*'([^']*)'\s*$"
                ).unwrap();
                if parameters_re.is_match(&parameters) {
                    let cap = parameters_re
                        .captures(&parameters)
                        .expect("Match asProcedure parameters error");
                    let query_name = cap[0].to_string();
                    let query = cap[1].to_string();
                    let mode = cap[2].to_string();
                    let outputs = cap[3].to_string();
                    let inputs = cap[4].to_string();
                    let description = cap[5].to_string();
                } else {
                    let reply = pb::CallResponse {
                        is_success: false,
                        results: vec![],
                        reason: format!(
                            "Fail to parse parameters for procedure: gs.flex.custom.asProcedure"
                        ),
                    };
                }
            }
            "gs.flex.custom.defPrecompute" => {
                let parameters_re = Regex::new(
                    r"^\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*(\[(?:\['[^']*'(?:,\s*'[^']*')*\]\
                           (?:,\s*)?)*\])\s*,\s*(\[(?:\['[^']*'(?:,\s*'[^']*')*\](?:,\s*)?)*\])\s*,\s*'([^']*)'\s*$"
                ).unwrap();
                if parameters_re.is_match(&parameters) {
                    let cap = parameters_re
                        .captures(&parameters)
                        .expect("Match asProcedure parameters error");
                    let query_name = cap[0].to_string();
                    let query = cap[1].to_string();
                    let mode = cap[2].to_string();
                    let outputs = cap[3].to_string();
                    let inputs = cap[4].to_string();
                    let description = cap[5].to_string();
                } else {
                    let reply = pb::CallResponse {
                        is_success: false,
                        results: vec![],
                        reason: format!(
                            "Fail to parse parameters for procedure: gs.flex.custom.defPrecompute"
                        ),
                    };
                }
            }
            "gs.flex.CSRStore.batch_insert_vertices" => {
                let parameters_re = Regex::new(r#""([^"]*)"\s*,\s*"([^"]*)"\s*,\s*"([^"]*)""#).unwrap();
                if parameters_re.is_match(&parameters) {
                    let cap = parameters_re
                        .captures(&parameters)
                        .expect("Match batch insert vertices error");
                    let label = cap[1].to_string();
                    let filename = cap[2].to_string();
                    let properties = cap[3].to_string();
                    let data_root = "";
                    let data_root_path = PathBuf::from(data_root);

                    let mut graph_modifier = GraphModifier::new(&data_root_path);

                    graph_modifier.skip_header();
                    graph_modifier.parallel(self.workers);
                    let mut graph = self.graph_db.write().unwrap();
                    println!(
                        "insert vertices: label: {}, filename: {}, properties: {}",
                        label, filename, properties
                    );
                    graph_modifier
                        .apply_vertices_insert_with_filename(&mut graph, &label, &filename, &properties)
                        .unwrap();

                    let reply =
                        pb::CallResponse { is_success: true, results: vec![], reason: "".to_string() };
                    return Ok(Response::new(reply));
                } else {
                    let reply = pb::CallResponse {
                        is_success: false,
                        results: vec![],
                        reason: format!(
                            "Fail to parse parameters for procedure: gs.flex.CSRStore.batch_insert_vertices"
                        ),
                    };
                    return Ok(Response::new(reply));
                }
            }
            "gs.flex.CSRStore.batch_insert_edges" => {
                let parameters_re = Regex::new(r#""([^"]*)"\s*,\s*"([^"]*)"\s*,\s*"([^"]*)""#).unwrap();
                if parameters_re.is_match(&parameters) {
                    let cap = parameters_re
                        .captures(&parameters)
                        .expect("Match batch insert edges error");
                    let label = cap[1].to_string();
                    let filename = cap[2].to_string();
                    let properties = cap[3].to_string();
                    let data_root = "";
                    let data_root_path = PathBuf::from(data_root);

                    let mut graph_modifier = GraphModifier::new(&data_root_path);

                    graph_modifier.skip_header();
                    graph_modifier.parallel(self.workers);
                    let mut graph = self.graph_db.write().unwrap();
                    println!(
                        "insert edges: label: {}, filename: {}, properties: {}",
                        label, filename, properties
                    );
                    graph_modifier
                        .apply_edges_insert_with_filename(&mut graph, &label, &filename, &properties)
                        .unwrap();

                    let reply =
                        pb::CallResponse { is_success: true, results: vec![], reason: "".to_string() };
                    return Ok(Response::new(reply));
                } else {
                    let reply = pb::CallResponse {
                        is_success: false,
                        results: vec![],
                        reason: format!(
                            "Fail to parse parameters for procedure: gs.flex.CSRStore.batch_insert_edges"
                        ),
                    };
                    return Ok(Response::new(reply));
                }
            }
            "gs.flex.CSRStore.batch_delete_vertices" => {
                let parameters_re = Regex::new(r#""([^"]*)"\s*,\s*"([^"]*)"\s*,\s*"([^"]*)""#).unwrap();
                if parameters_re.is_match(&parameters) {
                    let cap = parameters_re
                        .captures(&parameters)
                        .expect("Match batch delete vertices error");
                    let label = cap[1].to_string();
                    let filename = cap[2].to_string();
                    let properties = cap[3].to_string();
                    let data_root = "";
                    let data_root_path = PathBuf::from(data_root);

                    let mut graph_modifier = GraphModifier::new(&data_root_path);

                    graph_modifier.skip_header();
                    graph_modifier.parallel(self.workers);
                    let mut graph = self.graph_db.write().unwrap();
                    println!(
                        "delete vertices: label: {}, filename: {}, properties: {}",
                        label, filename, properties
                    );
                    graph_modifier
                        .apply_vertices_delete_with_filename(&mut graph, &label, &filename, &properties)
                        .unwrap();

                    let reply =
                        pb::CallResponse { is_success: true, results: vec![], reason: "".to_string() };
                    return Ok(Response::new(reply));
                } else {
                    let reply = pb::CallResponse {
                        is_success: false,
                        results: vec![],
                        reason: format!(
                            "Fail to parse parameters for procedure: gs.flex.CSRStore.batch_delete_vertices"
                        ),
                    };
                    return Ok(Response::new(reply));
                }
            }
            "gs.flex.CSRStore.batch_delete_edges" => {
                let parameters_re = Regex::new(r#""([^"]*)"\s*,\s*"([^"]*)"\s*,\s*"([^"]*)""#).unwrap();
                if parameters_re.is_match(&parameters) {
                    let cap = parameters_re
                        .captures(&parameters)
                        .expect("Match batch delete edges error");
                    let label = cap[1].to_string();
                    let filename = cap[2].to_string();
                    let properties = cap[3].to_string();
                    let data_root = "";
                    let data_root_path = PathBuf::from(data_root);

                    let mut graph_modifier = GraphModifier::new(&data_root_path);

                    graph_modifier.skip_header();
                    graph_modifier.parallel(self.workers);
                    let mut graph = self.graph_db.write().unwrap();
                    println!(
                        "delete edges: label: {}, filename: {}, properties: {}",
                        label, filename, properties
                    );
                    graph_modifier
                        .apply_edges_delete_with_filename(&mut graph, &label, &filename, &properties)
                        .unwrap();

                    let reply =
                        pb::CallResponse { is_success: true, results: vec![], reason: "".to_string() };
                    return Ok(Response::new(reply));
                } else {
                    let reply = pb::CallResponse {
                        is_success: false,
                        results: vec![],
                        reason: format!(
                            "Fail to parse parameters for procedure: gs.flex.CSRStore.batch_delete_edges"
                        ),
                    };
                    return Ok(Response::new(reply));
                }
            }
            _ => {
                let query_name_re = Regex::new(r"custom.(\S*)").unwrap();
                if query_name_re.is_match(&function_name) {
                    let cap = query_name_re
                        .captures(&function_name)
                        .expect("Fail to match query name");
                    let query_name = cap[1].to_string();
                    if let Some((precompute_setting, precompute)) = self
                        .query_register
                        .get_precompute_vertex(&query_name)
                    {
                        info!("111");
                    } else if let Some((precompute_setting, precompute)) = self
                        .query_register
                        .get_precompute_vertex(&query_name)
                    {
                        info!("222");
                    } else if let Some(query) = self.query_register.get_query(&query_name) {
                        let mut parameters_map = HashMap::<String, String>::new();
                        let parameter_re = Regex::new(r#""([^"]*)"(?:,|$)"#).unwrap();
                        for caps in parameter_re.captures_iter(&parameters) {
                            if let Some(matched) = caps.get(1) {
                                println!("Matched parameter: {}", matched.as_str());
                            }
                        }
                    }
                } else {
                    let reply = pb::CallResponse {
                        is_success: false,
                        results: vec![],
                        reason: format!("Unknown procedure name: {}", function_name),
                    };
                    return Ok(Response::new(reply));
                }
            }
        }
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
