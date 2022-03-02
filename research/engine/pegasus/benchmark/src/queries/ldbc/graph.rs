use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::*;
use pegasus::configure_with_default;
use std::path::Path;

lazy_static! {
    pub static ref GRAPH: LargeGraphDB<DefaultId, InternalId> = _init_graph();
    pub static ref DATA_PATH: String = configure_with_default!(String, "DATA_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
}

fn _init_graph() -> LargeGraphDB<DefaultId, InternalId> {
    println!("Read the graph data from {:?} for demo.", *DATA_PATH);
    GraphDBConfig::default()
        .root_dir(&(*DATA_PATH))
        .partition(*PARTITION_ID)
        .schema_file(
            &(DATA_PATH.as_ref() as &Path)
                .join(DIR_GRAPH_SCHEMA)
                .join(FILE_SCHEMA),
        )
        .open()
        .expect("Open graph error")
}
