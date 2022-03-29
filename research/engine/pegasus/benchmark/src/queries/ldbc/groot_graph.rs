use maxgraph_common::util::{fs, Timer};
use maxgraph_store::api::GlobalGraphQuery;
use maxgraph_store::db::api::multi_version_graph::MultiVersionGraph;
use maxgraph_store::db::api::{GraphConfigBuilder, TypeDefBuilder, Value, ValueType};
use maxgraph_store::db::graph::store::GraphStore;
use maxgraph_store::groot::global_graph::GlobalGraph;
use pegasus::configure_with_default;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::env;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

lazy_static! {
    pub static ref GRAPH: GlobalGraph = _init_graph();
    pub static ref DATA_PATH: String = configure_with_default!(String, "DATA_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
}

fn _init_graph() -> GlobalGraph {
    println!("Read the graph data from {:?} for demo.", *DATA_PATH);
    let path = DATA_PATH;
    let mut builder = GraphConfigBuilder::new();
    builder.set_storage_engine("rocksdb");
    builder.add_storage_option("store.rocksdb.stats.dump.period.sec", "60");
    let config = builder.build();
    let store = Arc::new(GraphStore::open(&config, &path).unwrap());
    println!("store opened.");
    let mut graph = GlobalGraph::empty(1);
    graph.add_partition(0, store);
    graph
}
