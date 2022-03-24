use maxgraph_common::util::{fs, Timer};
use maxgraph_store::api::GlobalGraphQuery;
use maxgraph_store::db::api::multi_version_graph::MultiVersionGraph;
use maxgraph_store::db::api::{GraphConfigBuilder, TypeDefBuilder, Value, ValueType};
use maxgraph_store::db::graph::store::GraphStore;
use maxgraph_store::groot::global_graph::GlobalGraph;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::env;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn main() {
    let args: Vec<String> = env::args().collect();

    let path = format!("/home/GraphScope/interactive_engine/distribution/target/maxgraph/data/0");
    let mut builder = GraphConfigBuilder::new();
    builder.set_storage_engine("rocksdb");
    builder.add_storage_option("store.rocksdb.compression.type", "none");
    builder.add_storage_option("store.rocksdb.stats.dump.period.sec", "60");
    let config = builder.build();
    let store = Arc::new(GraphStore::open(&config, &path).unwrap());
    println!("store opened.");
    let mut graph = GlobalGraph::empty(1);
    graph.add_partition(0, store);
    for (v, vi) in graph.get_out_vertex_ids(100, vec![(0, vec![933])], &vec![8], None, None, 20) {
        println!("{}", v);
        for i in vi {
            println!("{:?}", i)
        }
    }
}
