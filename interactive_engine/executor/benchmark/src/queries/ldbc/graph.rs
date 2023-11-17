extern crate chrono;

use std::path::Path;

use chrono::offset::{TimeZone, Utc};
use chrono::DateTime;
use mcsr::graph_db_impl::{CsrDB, SingleSubGraph, SubGraph};
use pegasus::configure_with_default;

use self::chrono::Datelike;

lazy_static! {
    pub static ref CSR: CsrDB<usize, usize> = _init_csr();
    pub static ref CSR_PATH: String = configure_with_default!(String, "CSR_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
    pub static ref COMMENT_REPLYOF_COMMENT_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(2, 3, 2, mcsr::graph::Direction::Incoming);
    pub static ref COMMENT_REPLYOF_POST_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(3, 3, 2, mcsr::graph::Direction::Incoming);
    pub static ref COMMENT_REPLYOF_COMMENT_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(2, 3, 2, mcsr::graph::Direction::Outgoing);
    pub static ref COMMENT_REPLYOF_POST_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(2, 3, 3, mcsr::graph::Direction::Outgoing);
    pub static ref COMMENT_HASTAG_TAG_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(7, 1, 2, mcsr::graph::Direction::Incoming);
    pub static ref POST_HASTAG_TAG_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(7, 1, 3, mcsr::graph::Direction::Incoming);
    pub static ref FORUM_HASTAG_TAG_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(7, 1, 4, mcsr::graph::Direction::Incoming);
    pub static ref COMMENT_HASTAG_TAG_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(2, 1, 7, mcsr::graph::Direction::Outgoing);
    pub static ref POST_HASTAG_TAG_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(3, 1, 7, mcsr::graph::Direction::Outgoing);
    pub static ref FORUM_HASMODERATOR_PERSON_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 6, 4, mcsr::graph::Direction::Incoming);
    pub static ref FORUM_HASMODERATOR_PERSON_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(4, 6, 1, mcsr::graph::Direction::Outgoing);
    pub static ref FORUM_HASMEMBER_PERSON_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 5, 4, mcsr::graph::Direction::Incoming);
    pub static ref FORUM_HASMEMBER_PERSON_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(4, 5, 1, mcsr::graph::Direction::Outgoing);
    pub static ref FORUM_CONTAINEROF_POST_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(4, 4, 3, mcsr::graph::Direction::Outgoing);
    pub static ref FORUM_CONTAINEROF_POST_IN: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(3, 4, 4, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_ISLOCATEDIN_PLACE_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(0, 2, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_ISLOCATEDIN_PLACE_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(1, 2, 0, mcsr::graph::Direction::Outgoing);
    pub static ref COMMENT_HASCREATOR_PERSON_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(2, 0, 1, mcsr::graph::Direction::Outgoing);
    pub static ref POST_HASCREATOR_PERSON_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(3, 0, 1, mcsr::graph::Direction::Outgoing);
    pub static ref COMMENT_HASCREATOR_PERSON_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 0, 2, mcsr::graph::Direction::Incoming);
    pub static ref POST_HASCREATOR_PERSON_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 0, 3, mcsr::graph::Direction::Incoming);
    pub static ref TAG_HASTYPE_TAGCLASS_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(6, 14, 7, mcsr::graph::Direction::Incoming);
    pub static ref PLACE_ISPARTOF_PLACE_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(0, 12, 0, mcsr::graph::Direction::Incoming);
    pub static ref PLACE_ISPARTOF_PLACE_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(0, 12, 0, mcsr::graph::Direction::Outgoing);
    pub static ref PERSON_LIKES_COMMENT_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(2, 9, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_LIKES_POST_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(3, 9, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_LIKES_COMMENT_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 9, 2, mcsr::graph::Direction::Outgoing);
    pub static ref PERSON_LIKES_POST_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 9, 3, mcsr::graph::Direction::Outgoing);
    pub static ref PERSON_HASINTEREST_TAG_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(7, 7, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_KNOWS_PERSON_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 8, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_KNOWS_PERSON_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 8, 1, mcsr::graph::Direction::Outgoing);
    pub static ref PERSON_WORKAT_ORGANISATION_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 11, 5, mcsr::graph::Direction::Outgoing);
    pub static ref PERSON_WORKAT_ORGANISATION_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(5, 11, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_STUDYAT_ORGANISATION_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 10, 5, mcsr::graph::Direction::Outgoing);
}

fn _init_csr() -> CsrDB<usize, usize> {
    // CsrDB::import(&*(CSR_PATH), *PARTITION_ID).unwrap()
    CsrDB::deserialize(&*(CSR_PATH), *PARTITION_ID).unwrap()
}

/// Typical symbols that split a string-format of a time data.
fn is_time_splitter(c: char) -> bool {
    c == '-' || c == ':' || c == ' ' || c == 'T' || c == 'Z' || c == '.'
}

pub fn global_id_to_id(global_id: u64, label_id: u8) -> u64 {
    global_id ^ ((label_id as u64) << 56)
}

pub fn get_partition(id: &u64, workers: usize, num_servers: usize) -> u64 {
    let id_usize = *id as usize;
    let magic_num = id_usize / num_servers;
    // The partitioning logics is as follows:
    // 1. `R = id - magic_num * num_servers = id % num_servers` routes a given id
    // to the machine R that holds its data.
    // 2. `R * workers` shifts the worker's id in the machine R.
    // 3. `magic_num % workers` then picks up one of the workers in the machine R
    // to do the computation.
    ((id_usize - magic_num * num_servers) * workers + magic_num % workers) as u64
}

pub fn get_2d_partition(id_hi: u64, id_low: u64, workers: usize, num_servers: usize) -> u64 {
    let server_id = id_hi % num_servers as u64;
    let worker_id = id_low % workers as u64;
    server_id * workers as u64 + worker_id
}

pub fn get_csr_partition(id: &u64, workers: usize, num_servers: usize) -> u64 {
    let id_usize = *id as usize;
    let magic_num = id_usize / num_servers;
    // The partitioning logics is as follows:
    // 1. `R = id - magic_num * num_servers = id % num_servers` routes a given id
    // to the machine R that holds its data.
    // 2. `R * workers` shifts the worker's id in the machine R.
    // 3. `magic_num % workers` then picks up one of the workers in the machine R
    // to do the computation.
    ((id_usize - magic_num * num_servers) * workers + magic_num % workers) as u64
}
