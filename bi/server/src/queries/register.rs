use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use storage::graph_db::GraphDB;
use storage::graph_modifier::{AliasData, WriteOperation};

use crossbeam_channel::Sender;
use crossbeam_utils::sync::ShardedLock;
use dlopen::wrapper::{Container, WrapperApi};
use pegasus::api::*;
use pegasus::errors::BuildJobError;
use pegasus::result::ResultSink;
use pegasus::JobConf;
use pegasus_network::{InboxRegister, NetData};
use serde::{Deserialize, Serialize};

#[derive(WrapperApi)]
pub struct QueryApi {
    Query: fn(
        conf: JobConf,
        graph_db: &GraphDB<usize, usize>,
        input_params: HashMap<String, String>,
        msg_sender_map: Arc<ShardedLock<HashMap<(u64, u64), (SocketAddr, Weak<Sender<NetData>>)>>>,
        recv_register_map: Arc<ShardedLock<HashMap<(u64, u64), InboxRegister>>>,
    ) -> Box<
        dyn Fn(
            &mut Source<Vec<AliasData>>,
            ResultSink<(u32, Option<Vec<AliasData>>, Option<Vec<WriteOperation>>, Option<Vec<u8>>)>,
        ) -> Result<(), BuildJobError>,
    >,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Param {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueryConfig {
    pub name: String,
    pub description: String,
    pub mode: String,
    pub extension: String,
    pub library: String,
    pub params: Option<Vec<Param>>,
    pub returns: Option<Vec<Param>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueriesConfig {
    pub queries: Option<Vec<QueryConfig>>,
}

pub struct QueryRegister {
    query_map: HashMap<String, (String, String)>,
}

unsafe impl Send for QueryRegister {}

unsafe impl Sync for QueryRegister {}

impl QueryRegister {
    pub fn new() -> Self {
        Self { query_map: HashMap::new() }
    }

    pub fn load(&mut self, config_path: &PathBuf) {
        let file = File::open(config_path).expect("Failed to open config file");
        let queries_config: QueriesConfig = serde_yaml::from_reader(file).expect("Could not read values");
        if let Some(queries) = queries_config.queries {
            for query in queries {
                let query_name = query.name;
                let lib_path = query.library;
                let lib_type = query.mode;
                self.query_map
                    .insert(query_name.clone(), (lib_path.clone(), lib_type.clone()));
            }
        }
    }

    pub fn get_new_query(&self, query_name: &String) -> Option<(Vec<Container<QueryApi>>, String)> {
        if let Some((lib_path, lib_type)) = self.query_map.get(query_name) {
            Some((vec![unsafe { Container::load(lib_path) }.unwrap()], lib_type.clone()))
        } else {
            None
        }
    }
}
