use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Command;
use std::ptr::write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;
use std::{env, fs};

use bmcsr::graph::Direction;
use bmcsr::graph_db::GraphDB;
use bmcsr::graph_modifier::{DeleteGenerator, GraphModifier};
use bmcsr::ldbc_parser::LDBCVertexParser;
use bmcsr::schema::InputSchema;
use bmcsr::traverse::traverse;
use dlopen::wrapper::{Container, WrapperApi};
use futures::Stream;
use graph_index::types::*;
use graph_index::GraphIndex;
use hyper::server::accept::Accept;
use hyper::server::conn::{AddrIncoming, AddrStream};
use pegasus::api::function::FnResult;
use pegasus::api::FromStream;
use pegasus::resource::DistributedParResourceMaps;
use pegasus::result::{FromStreamExt, ResultSink};
use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_network::config::ServerAddr;
use regex::Regex;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::generated::protocol as pb;
use crate::queries::register::QueryRegister;
use crate::queries::write_graph;

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

static CODEGEN_TMP_DIR: &'static str = "CODEGEN_TMP_DIR";

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
impl pb::job_service_server::JobService for JobServiceImpl {
    async fn submit_call(
        &self, req: Request<pb::CallRequest>,
    ) -> Result<Response<pb::CallResponse>, Status> {
        let pb::CallRequest { query } = req.into_inner();
        let function_name_re = Regex::new(r"^CALL ([^\(]*)\(([\s\S]*?)\)$").unwrap();
        let (function_name, parameters) = if function_name_re.is_match(&query) {
            let capture = function_name_re
                .captures(&query)
                .expect("Capture function_name error");
            println!("function_name: {}, parameters: {}", capture[1].to_string(), capture[2].to_string());
            (capture[1].to_string(), capture[2].to_string())
        } else {
            let reply = pb::CallResponse { is_success: false, results: vec![], reason: "".to_string() };
            return Ok(Response::new(reply));
        };
        match function_name.as_str() {
            "gs.flex.custom.asProcedure" => {
                let parameters_re = Regex::new(r"^\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*\[((?:\['[^']*'(?:,\s*'[^']*')*\](?:,\s*)?)*)\]\s*,\s*(\[(?:\['[^']*'(?:,\s*'[^']*')*\](?:,\s*)?)*\])\s*,\s*'([^']*)'\s*$").unwrap();
                if parameters_re.is_match(&parameters) {
                    let cap = parameters_re
                        .captures(&parameters)
                        .expect("Match asProcedure parameters error");
                    let query_name = cap[1].to_string();
                    let query = cap[2].to_string();
                    let mode = cap[3].to_string();
                    let outputs = cap[4].to_string();
                    let inputs = cap[5].to_string();
                    let description = cap[6].to_string();
                    let mut inputs_info = vec![];
                    let mut outputs_info = HashMap::new();
                    let input_re = Regex::new(r"\['([^']*)',\s*'([^']*)'\]").unwrap();
                    for cap in input_re.captures_iter(&inputs) {
                        inputs_info.push((cap[1].to_string(), cap[2].to_string()));
                    }
                    let output_re = Regex::new(r"\['([^']*)',\s*'([^']*)'\]").unwrap();
                    for cap in output_re.captures_iter(&outputs) {
                        outputs_info.insert(cap[1].to_string(), cap[2].to_string());
                    }
                    let exe_path = env::current_exe()?;
                    let exe_dir = exe_path
                        .parent()
                        .unwrap_or_else(|| {
                            panic!("无法获取可执行文件的目录");
                        })
                        .to_path_buf();
                    let gie_dir = exe_path
                        .parent()
                        .and_then(|p| p.parent())
                        .and_then(|p| p.parent())
                        .and_then(|p| p.parent())
                        .and_then(|p| p.parent())
                        .and_then(|p| p.parent())
                        .and_then(|p| p.parent())
                        .unwrap_or_else(|| panic!("Failed to find path to gie-codegen"));
                    let temp_dir = env::var(CODEGEN_TMP_DIR).unwrap_or_else(|_| format!("/tmp"));
                    let cypher_path = format!("{}/{}.cypher", temp_dir, query_name);
                    let mut cypher_file = match std::fs::File::create(&Path::new(cypher_path.as_str())) {
                        Err(reason) => panic!("Failed to create file {}: {}", cypher_path, reason),
                        Ok(file) => file,
                    };
                    cypher_file
                        .write_all(query.as_bytes())
                        .expect("Failed to write query to file");
                    let plan_path = format!("{}/{}.plan", temp_dir, query_name);
                    let config_path = format!("{}/{}.yaml", temp_dir, query_name);
                    let gie_dir_str = gie_dir.to_str().unwrap();

                    // Run compiler
                    let compiler_status = Command::new("java")
                        .arg("-cp")
                        .arg(format!("{}/GraphScope/interactive_engine/compiler/target/compiler-0.0.1-SNAPSHOT-shade.jar", gie_dir_str))
                        .arg("-Djna.library.path=".to_owned() + gie_dir_str + "/GraphScope/interactive_engine/executor/ir/target/release/")
                        .arg("com.alibaba.graphscope.common.ir.tools.GraphPlanner")
                        .arg(format!("{}/GraphScope/interactive_engine/compiler/conf/ir.compiler.properties", gie_dir_str))
                        .arg(cypher_path)
                        .arg(plan_path)
                        .arg(config_path)
                        .arg(format!("name:{}", query_name)).status();
                    match compiler_status {
                        Ok(status) => {
                            println!("Finished generate plan for query {}", query_name);
                        }
                        Err(e) => {
                            // 处理运行命令的错误
                            eprintln!("Error executing command: {}", e);
                        }
                    }

                    // Run codegen
                    let codegen_status =
                        Command::new(format!("{}/build/gen_pegasus_from_plan", gie_dir_str))
                            .arg("-i")
                            .arg(format!("{}/{}.plan", temp_dir, query_name))
                            .arg("-n")
                            .arg(query_name.as_str())
                            .arg("-t")
                            .arg("plan")
                            .arg("-s")
                            .arg(format!(
                                "{}/GraphScope/interactive_engine/executor/store/bmcsr/schema.json",
                                gie_dir_str
                            ))
                            .arg("-r")
                            .arg("single_machine")
                            .status();
                    match codegen_status {
                        Ok(status) => {
                            println!("Finished codegen for query {}", query_name);
                        }
                        Err(e) => {
                            // 处理运行命令的错误
                            eprintln!("Error executing command: {}", e);
                        }
                    }

                    // Build so
                    let query_project = format!("{}/benchmark/{}/src", gie_dir_str, query_name);
                    let query_project_path = Path::new(query_project.as_str());
                    if !query_project_path.exists() {
                        fs::create_dir_all(&query_project_path).expect("Failed to create project dir");
                    }
                    let codegen_path = format!("{}/{}.rs", temp_dir, query_name);
                    let lib_path = format!("{}/lib.rs", query_project);
                    fs::copy(codegen_path, lib_path).expect("Failed to copy rust code");

                    let cargo_template_path = format!("{}/benchmark/Cargo.toml.template", gie_dir_str);
                    let mut cargo_toml_contents = fs::read_to_string(cargo_template_path)?;
                    cargo_toml_contents = cargo_toml_contents.replace("${query_name}", query_name.as_str());
                    let cargo_toml_path = format!("{}/benchmark/{}/Cargo.toml", gie_dir_str, query_name);
                    fs::write(cargo_toml_path, cargo_toml_contents).expect("Failed to write cargo file");

                    let build_status = Command::new("cargo")
                        .arg("build")
                        .arg("--release")
                        .current_dir(format!("{}/benchmark/{}", gie_dir_str, query_name))
                        .status();
                    match build_status {
                        Ok(status) => {
                            println!("Finished build dylib for query {}", query_name);
                        }
                        Err(e) => {
                            // 处理运行命令的错误
                            eprintln!("Error executing command: {}", e);
                        }
                    }
                    let dylib_path = format!(
                        "{}/benchmark/{}/target/release/lib{}.so",
                        gie_dir_str, query_name, query_name
                    );
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
                    r"^\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*(\[(?:\['[^']*'(?:,\s*'[^']*')*\](?:,\s*)?)*\])\s*$"
                ).unwrap();
                if parameters_re.is_match(&parameters) {
                    let cap = parameters_re
                        .captures(&parameters)
                        .expect("Match defPrecompute parameters error");
                    let query_name = cap[1].to_string();
                    let target = cap[2].to_string();
                    let mappings = cap[3].to_string();
                    let mut mappings_list = vec![];
                    let mappings_re = Regex::new(r"\['([^']*)',\s*'([^']*)',\s*'([^']*)'\]").unwrap();
                    for cap in mappings_re.captures_iter(&mappings) {
                        mappings_list.push((cap[1].to_string(), cap[2].to_string(), cap[3].to_string()));
                    }
                    if let Some(outputs_info) = self
                        .query_register
                        .get_query_outputs_info(&query_name)
                    {
                        let vertex_re = Regex::new(r"^\s*\((\w+)\s*:\s*(\w+)\)\s*$").unwrap();
                        let edge_re = Regex::new(
                            r"\((\w+):\s*(\w+)\)((-|<-\[|\]-\])(\w+):(\w+)(\]|->|-\]))\((\w+): (\w+)\)",
                        )
                        .unwrap();
                        if vertex_re.is_match(&target) {
                            let cap = vertex_re
                                .captures(&target)
                                .expect("Failed to parse target");
                            let alias = cap[1].to_string();
                            let label_name = cap[2].to_string();
                            let (label_id, property_size) = {
                                let graph = self.graph_db.read().unwrap();
                                if let Some(label_id) = graph
                                    .graph_schema
                                    .vertex_type_to_id
                                    .get(&label_name)
                                {
                                    let property_size = graph.get_vertices_num(*label_id);
                                    (*label_id, property_size)
                                } else {
                                    let reply = pb::CallResponse {
                                        is_success: false,
                                        results: vec![],
                                        reason: format!("Invalid vertex label name: {}", label_name),
                                    };
                                    return Ok(Response::new(reply));
                                }
                            };
                            let mut precompute_info = vec![];
                            for i in mappings_list {
                                let precompute_name = i.0.split('.').collect::<Vec<&str>>()[1].to_string();
                                if let Some(info) = outputs_info.get(&i.1) {
                                    let data_type = graph_index::types::str_to_data_type(info);
                                    let default_value =
                                        graph_index::types::str_to_default_value(&i.2, data_type);
                                    if precompute_name != "id" {
                                        let mut graph_index = self
                                            .graph_index
                                            .write()
                                            .expect("Graph index poisoned");
                                        graph_index.init_vertex_index(
                                            precompute_name.clone(),
                                            label_id,
                                            data_type,
                                            Some(property_size),
                                            Some(default_value),
                                        );
                                    }
                                    precompute_info.push((precompute_name, data_type));
                                } else {
                                    let reply = pb::CallResponse {
                                        is_success: false,
                                        results: vec![],
                                        reason: format!("Unknown results in outputs"),
                                    };
                                    return Ok(Response::new(reply));
                                }
                            }
                            self.query_register.register_vertex_precompute(
                                query_name,
                                label_id,
                                precompute_info,
                            );
                        } else if edge_re.is_match(&target) {
                        } else {
                        }
                    } else {
                    }
                } else {
                    let reply = pb::CallResponse {
                        is_success: false,
                        results: vec![],
                        reason: format!(
                            "Fail to parse parameters for procedure: gs.flex.custom.defPrecompute"
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
                    // graph_modifier
                    //     .apply_edges_insert_with_filename(&mut graph, &label, &filename, &properties)
                    //     .unwrap();
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
                    // graph_modifier
                    //     .apply_vertices_delete_with_filename(&mut graph, &label, &filename, &properties)
                    //     .unwrap();

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
                    // graph_modifier
                    //     .apply_edges_delete_with_filename(&mut graph, &label, &filename, &properties)
                    //     .unwrap();

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
                    println!("Start to run query {}", function_name);
                    let cap = query_name_re
                        .captures(&function_name)
                        .expect("Fail to match query name");
                    let query_name = cap[1].to_string();
                    if let Some(queries) = self.query_register.get_new_query(&query_name) {
                        // Run queries
                        if let Some(inputs_info) = self
                            .query_register
                            .get_query_inputs_info(&query_name)
                        {
                            let mut parameters_map = HashMap::<String, String>::new();
                            let parameter_re = Regex::new(r#""([^"]*)"(?:,|$)"#).unwrap();
                            let mut input_index = 0;
                            for caps in parameter_re.captures_iter(&parameters) {
                                if let Some(matched) = caps.get(1) {
                                    parameters_map.insert(
                                        inputs_info[input_index].0.clone(),
                                        matched.as_str().to_string(),
                                    );
                                    input_index += 1;
                                }
                            }

                            let mut index = 0;
                            // let mut query_results = vec![];
                            let resource_maps = DistributedParResourceMaps::default(
                                ServerConf::Partial(self.servers.clone()),
                                self.workers,
                            );
                            let alias_data = Arc::new(Mutex::new(HashMap::new()));
                            for query in queries.iter() {
                                let mut conf = JobConf::new(format!("{}_{}", query_name, index));
                                conf.set_workers(self.workers);
                                conf.reset_servers(ServerConf::Partial(self.servers.clone()));
                                let graph = self.graph_db.read().unwrap();
                                let graph_index = self.graph_index.read().unwrap();
                                let results = {
                                    pegasus::run_with_resource_map(
                                        conf.clone(),
                                        Some(resource_maps.clone()),
                                        || {
                                            query.Query(
                                                conf.clone(),
                                                &graph,
                                                &graph_index,
                                                parameters_map.clone(),
                                                Some(alias_data.clone()),
                                            )
                                        },
                                    )
                                    .expect("submit query failure")
                                };
                                let mut write_operations = vec![];
                                let mut alias_data_write = alias_data
                                    .lock()
                                    .expect("Mutex of alias data poisoned");
                                alias_data_write.clear();
                                for result in results {
                                    if let Ok((worker_id, alias_datas, write_ops, query_result)) = result {
                                        if let Some(alias_datas) = alias_datas {
                                            alias_data_write.insert(worker_id, alias_datas);
                                        }
                                        if let Some(write_ops) = write_ops {
                                            for write_op in write_ops {
                                                write_operations.push(write_op);
                                            }
                                        }
                                    }
                                }
                                drop(alias_data_write);
                                drop(graph);
                                let mut graph = self.graph_db.write().unwrap();
                                let mut graph_index = self.graph_index.write().unwrap();
                                for write_op in write_operations.drain(..) {
                                    match write_op.write_type() {
                                        WriteType::Insert => {
                                            if let Some(vertex_mappings) = write_op.vertex_mappings() {
                                                let vertex_label = vertex_mappings.vertex_label();
                                                let inputs = vertex_mappings.inputs();
                                                let column_mappings = vertex_mappings.column_mappings();
                                                for input in inputs.iter() {
                                                    write_graph::insert_vertices(
                                                        &mut graph,
                                                        vertex_label,
                                                        input,
                                                        column_mappings,
                                                        self.workers,
                                                    );
                                                }
                                            }
                                            if let Some(edge_mappings) = write_op.edge_mappings() {
                                                let src_label = edge_mappings.src_label();
                                                let edge_label = edge_mappings.edge_label();
                                                let dst_label = edge_mappings.dst_label();
                                                let inputs = edge_mappings.inputs();
                                                let src_column_mappings =
                                                    edge_mappings.src_column_mappings();
                                                let dst_column_mappings =
                                                    edge_mappings.dst_column_mappings();
                                                let column_mappings = edge_mappings.column_mappings();
                                                for input in inputs.iter() {
                                                    write_graph::insert_edges(
                                                        &mut graph,
                                                        src_label,
                                                        edge_label,
                                                        dst_label,
                                                        input,
                                                        src_column_mappings,
                                                        dst_column_mappings,
                                                        column_mappings,
                                                        self.workers,
                                                    );
                                                }
                                            }
                                        }
                                        WriteType::Delete => {
                                            if let Some(vertex_mappings) = write_op.vertex_mappings() {
                                                let vertex_label = vertex_mappings.vertex_label();
                                                let inputs = vertex_mappings.inputs();
                                                let column_mappings = vertex_mappings.column_mappings();
                                                for input in inputs.iter() {
                                                    write_graph::delete_vertices(
                                                        &mut graph,
                                                        vertex_label,
                                                        input,
                                                        column_mappings,
                                                        self.workers,
                                                    );
                                                }
                                            }
                                            if let Some(edge_mappings) = write_op.edge_mappings() {
                                                let src_label = edge_mappings.src_label();
                                                let edge_label = edge_mappings.edge_label();
                                                let dst_label = edge_mappings.dst_label();
                                                let inputs = edge_mappings.inputs();
                                                let src_column_mappings =
                                                    edge_mappings.src_column_mappings();
                                                let dst_column_mappings =
                                                    edge_mappings.dst_column_mappings();
                                                let column_mappings = edge_mappings.column_mappings();
                                                for input in inputs.iter() {
                                                    write_graph::delete_edges(
                                                        &mut graph,
                                                        src_label,
                                                        edge_label,
                                                        dst_label,
                                                        input,
                                                        src_column_mappings,
                                                        dst_column_mappings,
                                                        column_mappings,
                                                        self.workers,
                                                    );
                                                }
                                            }
                                        }
                                        WriteType::Set => {
                                            if let Some(vertex_mappings) = write_op.vertex_mappings() {
                                                let vertex_label = vertex_mappings.vertex_label();
                                                let inputs = vertex_mappings.inputs();
                                                let column_mappings = vertex_mappings.column_mappings();
                                                for input in inputs.iter() {
                                                    write_graph::set_vertices(
                                                        &mut graph,
                                                        &mut graph_index,
                                                        vertex_label,
                                                        input,
                                                        column_mappings,
                                                        self.workers,
                                                    );
                                                }
                                            }
                                        }
                                    };
                                }
                                index += 1;
                            }
                            drop(resource_maps);
                            let reply =
                                pb::CallResponse { is_success: true, results: vec![], reason: format!("") };
                            return Ok(Response::new(reply));
                        } else {
                            let reply = pb::CallResponse {
                                is_success: false,
                                results: vec![],
                                reason: format!("Failed to get inputs info of query: {}", function_name),
                            };
                            return Ok(Response::new(reply));
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
