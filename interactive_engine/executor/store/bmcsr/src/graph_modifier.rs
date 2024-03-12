use crate::col_table::{parse_properties, ColTable};
use crate::columns::{Column, StringColumn};
use csv::ReaderBuilder;
use rayon::iter::IntoParallelIterator;
use rayon::prelude::*;
use rust_htslib::bgzf::Reader as GzReader;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

use crate::bmcsr::{BatchMutableCsr, BatchMutableCsrBuilder};
use crate::bmscsr::BatchMutableSingleCsr;
use crate::csr::CsrTrait;
use crate::error::GDBResult;
use crate::graph::{Direction, IndexType};
use crate::graph_db::GraphDB;
use crate::graph_loader::{get_files_list, get_files_list_beta};
use crate::ldbc_parser::{LDBCEdgeParser, LDBCVertexParser};
use crate::schema::{CsrGraphSchema, InputSchema, Schema};
use crate::types::{DefaultId, LabelId};

pub struct DeleteGenerator<G: FromStr + Send + Sync + IndexType + std::fmt::Display = DefaultId> {
    input_dir: PathBuf,

    delim: u8,
    skip_header: bool,

    persons: Vec<(String, G)>,
    comments: Vec<(String, G)>,
    posts: Vec<(String, G)>,
    forums: Vec<(String, G)>,

    person_set: HashSet<G>,
    comment_set: HashSet<G>,
    post_set: HashSet<G>,
    forum_set: HashSet<G>,
}

impl<G: FromStr + Send + Sync + IndexType + Eq + std::fmt::Display> DeleteGenerator<G> {
    pub fn new(input_dir: &PathBuf) -> DeleteGenerator<G> {
        Self {
            input_dir: input_dir.clone(),
            delim: b'|',
            skip_header: false,

            persons: vec![],
            comments: vec![],
            posts: vec![],
            forums: vec![],

            person_set: HashSet::new(),
            comment_set: HashSet::new(),
            post_set: HashSet::new(),
            forum_set: HashSet::new(),
        }
    }

    fn load_vertices(&self, input_prefix: PathBuf, label: LabelId) -> Vec<(String, G)> {
        let mut ret = vec![];

        let suffixes = vec!["*.csv.gz".to_string(), "*.csv".to_string()];
        let files = get_files_list(&input_prefix, &suffixes);
        if files.is_err() {
            warn!(
                "Get vertex files {:?}/{:?} failed: {:?}",
                &input_prefix,
                &suffixes,
                files.err().unwrap()
            );
            return ret;
        }
        let files = files.unwrap();
        if files.is_empty() {
            return ret;
        }
        let parser = LDBCVertexParser::<G>::new(label, 1);
        for file in files {
            if file
                .clone()
                .to_str()
                .unwrap()
                .ends_with(".csv.gz")
            {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(b'|')
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(GzReader::from_path(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let vertex_meta = parser.parse_vertex_meta(&record);
                        ret.push((
                            record
                                .get(0)
                                .unwrap()
                                .parse::<String>()
                                .unwrap(),
                            vertex_meta.global_id,
                        ));
                    }
                }
            } else if file.clone().to_str().unwrap().ends_with(".csv") {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(b'|')
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(File::open(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let vertex_meta = parser.parse_vertex_meta(&record);
                        ret.push((
                            record
                                .get(0)
                                .unwrap()
                                .parse::<String>()
                                .unwrap(),
                            vertex_meta.global_id,
                        ));
                    }
                }
            }
        }

        ret
    }

    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn skip_header(&mut self) {
        self.skip_header = true;
    }

    fn iterate_persons<I>(&mut self, graph: &GraphDB<G, I>)
    where
        I: Send + Sync + IndexType,
    {
        let person_label = graph
            .graph_schema
            .get_vertex_label_id("PERSON")
            .unwrap();

        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();
        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        let forum_label = graph
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();

        let hasCreator_label = graph
            .graph_schema
            .get_edge_label_id("HASCREATOR")
            .unwrap();
        let hasModerator_label = graph
            .graph_schema
            .get_edge_label_id("HASMODERATOR")
            .unwrap();

        let comment_hasCreator_person =
            graph.get_sub_graph(person_label, hasCreator_label, comment_label, Direction::Incoming);
        let post_hasCreator_person =
            graph.get_sub_graph(person_label, hasCreator_label, post_label, Direction::Incoming);
        let forum_hasModerator_person =
            graph.get_sub_graph(person_label, hasModerator_label, forum_label, Direction::Incoming);

        let forum_title_column = graph.vertex_prop_table[forum_label as usize]
            .get_column_by_name("title")
            .as_any()
            .downcast_ref::<StringColumn>()
            .unwrap();

        for (dt, id) in self.persons.iter() {
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
                if got_label != person_label {
                    warn!("Vertex {} is not a person", LDBCVertexParser::<G>::get_original_id(*id));
                    continue;
                }
                for e in comment_hasCreator_person
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(comment_label, *e)
                        .unwrap();
                    self.comments.push((dt.clone(), oid));
                }

                for e in post_hasCreator_person
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(post_label, *e)
                        .unwrap();
                    self.posts.push((dt.clone(), oid));
                }

                for e in forum_hasModerator_person
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let title = forum_title_column.get(e.index()).unwrap();
                    let title_string = title.to_string();
                    if title_string.starts_with("Album") || title_string.starts_with("Wall") {
                        let oid = graph
                            .vertex_map
                            .get_global_id(forum_label, *e)
                            .unwrap();
                        self.forums.push((dt.clone(), oid));
                    }
                }
            } else {
                warn!("Vertex Person - {} does not exist", LDBCVertexParser::<G>::get_original_id(*id));
                continue;
            }
        }
    }

    fn iterate_forums<I>(&mut self, graph: &GraphDB<G, I>)
    where
        I: Send + Sync + IndexType,
    {
        let forum_label = graph
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();
        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();

        let containerOf_label = graph
            .graph_schema
            .get_edge_label_id("CONTAINEROF")
            .unwrap();

        let forum_containerOf_post =
            graph.get_sub_graph(forum_label, containerOf_label, post_label, Direction::Outgoing);
        for (dt, id) in self.forums.iter() {
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
                if got_label != forum_label {
                    warn!("Vertex {} is not a forum", LDBCVertexParser::<G>::get_original_id(*id));
                    continue;
                }

                for e in forum_containerOf_post
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(post_label, *e)
                        .unwrap();
                    self.posts.push((dt.clone(), oid));
                }
            } else {
                warn!("Vertex Forum - {} does not exist", LDBCVertexParser::<G>::get_original_id(*id));
                continue;
            }
        }
    }

    fn iterate_posts<I>(&mut self, graph: &GraphDB<G, I>)
    where
        I: Send + Sync + IndexType,
    {
        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();

        let replyOf_label = graph
            .graph_schema
            .get_edge_label_id("REPLYOF")
            .unwrap();

        let comment_replyOf_post =
            graph.get_sub_graph(post_label, replyOf_label, comment_label, Direction::Incoming);
        for (dt, id) in self.posts.iter() {
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
                if got_label != post_label {
                    warn!("Vertex {} is not a post", LDBCVertexParser::<G>::get_original_id(*id));
                    continue;
                }

                for e in comment_replyOf_post.get_adj_list(lid).unwrap() {
                    let oid = graph
                        .vertex_map
                        .get_global_id(comment_label, *e)
                        .unwrap();
                    self.comments.push((dt.clone(), oid));
                }
            } else {
                warn!("Vertex Post - {} does not exist", LDBCVertexParser::<G>::get_original_id(*id));
                continue;
            }
        }
    }

    fn iterate_comments<I>(&mut self, graph: &GraphDB<G, I>)
    where
        I: Send + Sync + IndexType,
    {
        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();

        let replyOf_label = graph
            .graph_schema
            .get_edge_label_id("REPLYOF")
            .unwrap();

        let comment_replyOf_comment =
            graph.get_sub_graph(comment_label, replyOf_label, comment_label, Direction::Incoming);
        let mut index = 0;
        while index < self.comments.len() {
            let (dt, id) = self.comments[index].clone();
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(id) {
                if got_label != comment_label {
                    warn!("Vertex {} is not a comment", LDBCVertexParser::<G>::get_original_id(id));
                    index += 1;
                    continue;
                }

                for e in comment_replyOf_comment
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(comment_label, *e)
                        .unwrap();
                    self.comments.push((dt.clone(), oid));
                }
                index += 1;
            } else {
                warn!("Vertex Comment - {} does not exist", LDBCVertexParser::<G>::get_original_id(id));
                index += 1;
                continue;
            }
        }
    }

    pub fn generate<I>(&mut self, graph: &GraphDB<G, I>, batch_id: &str)
    where
        I: Send + Sync + IndexType,
    {
        let output_dir = self
            .input_dir
            .join("extra_deletes")
            .join("dynamic");
        std::fs::create_dir_all(&output_dir).unwrap();

        let prefix = self.input_dir.join("deletes").join("dynamic");

        let person_label = graph
            .graph_schema
            .get_vertex_label_id("PERSON")
            .unwrap();
        self.persons = self.load_vertices(
            prefix
                .clone()
                .join("Person")
                .join(format!("batch_id={}", batch_id)),
            person_label,
        );
        self.person_set = self.persons.iter().map(|(_, id)| *id).collect();

        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();
        self.comments = self.load_vertices(
            prefix
                .clone()
                .join("Comment")
                .join(format!("batch_id={}", batch_id)),
            comment_label,
        );
        self.comment_set = self
            .comments
            .iter()
            .map(|(_, id)| *id)
            .collect();

        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        self.posts = self.load_vertices(
            prefix
                .clone()
                .join("Post")
                .join(format!("batch_id={}", batch_id)),
            post_label,
        );
        self.post_set = self.posts.iter().map(|(_, id)| *id).collect();

        let forum_label = graph
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();
        self.forums = self.load_vertices(
            prefix
                .clone()
                .join("Forum")
                .join(format!("batch_id={}", batch_id)),
            forum_label,
        );
        self.forum_set = self.forums.iter().map(|(_, id)| *id).collect();

        self.iterate_persons(graph);
        self.iterate_forums(graph);
        self.iterate_posts(graph);
        self.iterate_comments(graph);

        let batch_dir = format!("batch_id={}", batch_id);

        let person_dir_path = output_dir
            .clone()
            .join("Person")
            .join(&batch_dir);
        std::fs::create_dir_all(&person_dir_path).unwrap();
        let mut person_file = File::create(person_dir_path.join("part-0.csv")).unwrap();
        writeln!(person_file, "deletionDate|id").unwrap();
        for (dt, id) in self.persons.iter() {
            if !self.person_set.contains(id) {
                self.person_set.insert(*id);
                writeln!(person_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }

        let forum_dir_path = output_dir
            .clone()
            .join("Forum")
            .join(&batch_dir);
        std::fs::create_dir_all(&forum_dir_path).unwrap();
        let mut forum_file = File::create(forum_dir_path.join("part-0.csv")).unwrap();
        writeln!(forum_file, "deletionDate|id").unwrap();
        for (dt, id) in self.forums.iter() {
            if !self.forum_set.contains(id) {
                self.forum_set.insert(*id);
                writeln!(forum_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }

        let post_dir_path = output_dir.clone().join("Post").join(&batch_dir);
        std::fs::create_dir_all(&post_dir_path).unwrap();
        let mut post_file = File::create(post_dir_path.join("part-0.csv")).unwrap();
        writeln!(post_file, "deletionDate|id").unwrap();
        for (dt, id) in self.posts.iter() {
            if !self.post_set.contains(id) {
                self.post_set.insert(*id);
                writeln!(post_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }

        let comment_dir_path = output_dir
            .clone()
            .join("Comment")
            .join(&batch_dir);
        std::fs::create_dir_all(&comment_dir_path).unwrap();
        let mut comment_file = File::create(comment_dir_path.join("part-0.csv")).unwrap();
        writeln!(comment_file, "deletionDate|id").unwrap();
        for (dt, id) in self.comments.iter() {
            if !self.comment_set.contains(id) {
                self.comment_set.insert(*id);
                writeln!(comment_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }
    }
}

pub struct GraphModifier {
    input_dir: PathBuf,

    delim: u8,
    skip_header: bool,
    parallel: u32,
}

struct CsrRep<I> {
    src_label: LabelId,
    edge_label: LabelId,
    dst_label: LabelId,

    ie_csr: Box<dyn CsrTrait<I>>,
    ie_prop: Option<ColTable>,
    oe_csr: Box<dyn CsrTrait<I>>,
    oe_prop: Option<ColTable>,
}

impl GraphModifier {
    pub fn new<D: AsRef<Path>>(input_dir: D) -> GraphModifier {
        Self { input_dir: input_dir.as_ref().to_path_buf(), delim: b'|', skip_header: false, parallel: 0 }
    }

    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn skip_header(&mut self) {
        self.skip_header = true;
    }

    pub fn parallel(&mut self, parallel: u32) {
        self.parallel = parallel;
    }

    fn take_csrs<G, I>(&self, graph: &mut GraphDB<G, I>) -> Vec<CsrRep<I>>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let vertex_label_num = graph.vertex_label_num;
        let edge_label_num = graph.edge_label_num;
        let mut results = vec![];

        for e_label_i in 0..edge_label_num {
            for src_label_i in 0..vertex_label_num {
                for dst_label_i in 0..vertex_label_num {
                    if graph
                        .graph_schema
                        .get_edge_header(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                        )
                        .is_none()
                    {
                        continue;
                    }

                    let index = graph.edge_label_to_index(
                        src_label_i as LabelId,
                        dst_label_i as LabelId,
                        e_label_i as LabelId,
                        Direction::Outgoing,
                    );

                    results.push(CsrRep {
                        src_label: src_label_i as LabelId,
                        edge_label: e_label_i as LabelId,
                        dst_label: dst_label_i as LabelId,

                        ie_csr: std::mem::replace(
                            &mut graph.ie[index],
                            Box::new(BatchMutableSingleCsr::new()),
                        ),
                        ie_prop: graph.ie_edge_prop_table.remove(&index),
                        oe_csr: std::mem::replace(
                            &mut graph.oe[index],
                            Box::new(BatchMutableSingleCsr::new()),
                        ),
                        oe_prop: graph.oe_edge_prop_table.remove(&index),
                    });
                }
            }
        }

        results
    }

    fn set_csrs<G, I>(&self, graph: &mut GraphDB<G, I>, mut reps: Vec<CsrRep<I>>)
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        for result in reps.drain(..) {
            let index = graph.edge_label_to_index(
                result.src_label,
                result.dst_label,
                result.edge_label,
                Direction::Outgoing,
            );

            graph.ie[index] = result.ie_csr;
            if let Some(table) = result.ie_prop {
                graph.ie_edge_prop_table.insert(index, table);
            }
            graph.oe[index] = result.oe_csr;
            if let Some(table) = result.oe_prop {
                graph.oe_edge_prop_table.insert(index, table);
            }
        }
    }

    fn delete_rep<G, I>(
        &self, input: &mut CsrRep<I>, graph: &GraphDB<G, I>, input_schema: &InputSchema,
        delete_sets: &Vec<HashSet<I>>,
    ) where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let src_label = input.src_label;
        let edge_label = input.edge_label;
        let dst_label = input.dst_label;

        let graph_header = graph
            .graph_schema
            .get_edge_header(src_label, edge_label, dst_label);
        if graph_header.is_none() {
            return ();
        }

        let src_delete_set = &delete_sets[src_label as usize];
        let dst_delete_set = &delete_sets[dst_label as usize];
        let mut delete_edge_set = HashSet::new();

        if let Some(edge_file_strings) = input_schema.get_edge_file(src_label, edge_label, dst_label) {
            if let Some(input_header) = input_schema.get_edge_header(src_label, edge_label, dst_label) {
                let mut src_col_id = 0;
                let mut dst_col_id = 1;

                for (index, (n, _)) in input_header.iter().enumerate() {
                    if n == "start_id" {
                        src_col_id = index;
                    }
                    if n == "end_id" {
                        dst_col_id = index;
                    }
                }

                let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
                parser.with_endpoint_col_id(src_col_id, dst_col_id);

                let edge_files = get_files_list(&self.input_dir.clone(), edge_file_strings);
                if edge_files.is_err() {
                    return ();
                }

                let edge_files = edge_files.unwrap();
                for edge_file in edge_files.iter() {
                    if edge_file
                        .clone()
                        .to_str()
                        .unwrap()
                        .ends_with(".csv.gz")
                    {
                        let mut rdr = ReaderBuilder::new()
                            .delimiter(self.delim)
                            .buffer_capacity(4096)
                            .comment(Some(b'#'))
                            .flexible(true)
                            .has_headers(self.skip_header)
                            .from_reader(BufReader::new(GzReader::from_path(&edge_file).unwrap()));
                        for result in rdr.records() {
                            if let Ok(record) = result {
                                let edge_meta = parser.parse_edge_meta(&record);
                                let (got_src_label, src_lid) = graph
                                    .vertex_map
                                    .get_internal_id(edge_meta.src_global_id)
                                    .unwrap();
                                let (got_dst_label, dst_lid) = graph
                                    .vertex_map
                                    .get_internal_id(edge_meta.dst_global_id)
                                    .unwrap();
                                if got_src_label != src_label || got_dst_label != dst_label {
                                    warn!(
                                        "Edge - {} - {} does not exist",
                                        LDBCVertexParser::<G>::get_original_id(edge_meta.src_global_id)
                                            .index(),
                                        LDBCVertexParser::<G>::get_original_id(edge_meta.dst_global_id)
                                            .index()
                                    );
                                    continue;
                                }
                                if src_delete_set.contains(&src_lid) || dst_delete_set.contains(&dst_lid) {
                                    // warn!("Edge - {} - {} will be removed by vertices", LDBCVertexParser::<G>::get_original_id(edge_meta.src_global_id).index(), LDBCVertexParser::<G>::get_original_id(edge_meta.dst_global_id).index());
                                    continue;
                                }
                                delete_edge_set.insert((src_lid, dst_lid));
                            }
                        }
                    } else if edge_file
                        .clone()
                        .to_str()
                        .unwrap()
                        .ends_with(".csv")
                    {
                        let mut rdr = ReaderBuilder::new()
                            .delimiter(self.delim)
                            .buffer_capacity(4096)
                            .comment(Some(b'#'))
                            .flexible(true)
                            .has_headers(self.skip_header)
                            .from_reader(BufReader::new(File::open(&edge_file).unwrap()));
                        for result in rdr.records() {
                            if let Ok(record) = result {
                                let edge_meta = parser.parse_edge_meta(&record);
                                let (got_src_label, src_lid) = graph
                                    .vertex_map
                                    .get_internal_id(edge_meta.src_global_id)
                                    .unwrap();
                                let (got_dst_label, dst_lid) = graph
                                    .vertex_map
                                    .get_internal_id(edge_meta.dst_global_id)
                                    .unwrap();
                                if got_src_label != src_label || got_dst_label == dst_label {
                                    continue;
                                }
                                if src_delete_set.contains(&src_lid) || dst_delete_set.contains(&dst_lid) {
                                    continue;
                                }
                                delete_edge_set.insert((src_lid, dst_lid));
                            }
                        }
                    }
                }
            }
        }

        if src_delete_set.is_empty() && dst_delete_set.is_empty() && delete_edge_set.is_empty() {
            return ();
        }
        // println!(
        //     "Deleting edge - {} - {} - {}",
        //     graph.graph_schema.vertex_label_names()[src_label as usize],
        //     graph.graph_schema.edge_label_names()[edge_label as usize],
        //     graph.graph_schema.vertex_label_names()[dst_label as usize]
        // );

        let mut oe_to_delete = HashSet::new();
        let mut ie_to_delete = HashSet::new();

        for v in src_delete_set.iter() {
            if let Some(oe_list) = input.oe_csr.get_edges(*v) {
                for e in oe_list {
                    if !dst_delete_set.contains(e) {
                        oe_to_delete.insert((*v, *e));
                    }
                }
            }
        }
        for v in dst_delete_set.iter() {
            if let Some(ie_list) = input.ie_csr.get_edges(*v) {
                for e in ie_list {
                    if !src_delete_set.contains(e) {
                        ie_to_delete.insert((*e, *v));
                    }
                }
            }
        }

        input.oe_csr.delete_vertices(src_delete_set);
        if let Some(table) = input.oe_prop.as_mut() {
            input
                .oe_csr
                .delete_edges_with_props(&delete_edge_set, false, table);
            input
                .oe_csr
                .delete_edges_with_props(&ie_to_delete, false, table);
        } else {
            input
                .oe_csr
                .delete_edges(&delete_edge_set, false);
            input.oe_csr.delete_edges(&ie_to_delete, false);
        }

        input.ie_csr.delete_vertices(dst_delete_set);
        if let Some(table) = input.ie_prop.as_mut() {
            input
                .ie_csr
                .delete_edges_with_props(&delete_edge_set, true, table);
            input
                .ie_csr
                .delete_edges_with_props(&oe_to_delete, true, table);
        } else {
            input
                .ie_csr
                .delete_edges(&delete_edge_set, true);
            input.ie_csr.delete_edges(&oe_to_delete, true);
        }
    }

    fn parallel_delete_rep<G, I>(
        &self, input: &mut CsrRep<I>, graph: &GraphDB<G, I>, input_schema: &InputSchema,
        delete_sets: &Vec<HashSet<I>>, p: u32,
    ) where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let src_label = input.src_label;
        let edge_label = input.edge_label;
        let dst_label = input.dst_label;

        let graph_header = graph
            .graph_schema
            .get_edge_header(src_label, edge_label, dst_label);
        if graph_header.is_none() {
            return ();
        }

        let src_delete_set = &delete_sets[src_label as usize];
        let dst_delete_set = &delete_sets[dst_label as usize];
        let mut delete_edge_set = Vec::new();

        if let Some(edge_file_strings) = input_schema.get_edge_file(src_label, edge_label, dst_label) {
            if let Some(input_header) = input_schema.get_edge_header(src_label, edge_label, dst_label) {
                let mut src_col_id = 0;
                let mut dst_col_id = 1;

                for (index, (n, _)) in input_header.iter().enumerate() {
                    if n == "start_id" {
                        src_col_id = index;
                    }
                    if n == "end_id" {
                        dst_col_id = index;
                    }
                }

                let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
                parser.with_endpoint_col_id(src_col_id, dst_col_id);

                let edge_files = get_files_list(&self.input_dir.clone(), edge_file_strings);
                if edge_files.is_err() {
                    return ();
                }

                let edge_files = edge_files.unwrap();
                for edge_file in edge_files.iter() {
                    if edge_file
                        .clone()
                        .to_str()
                        .unwrap()
                        .ends_with(".csv.gz")
                    {
                        let mut rdr = ReaderBuilder::new()
                            .delimiter(self.delim)
                            .buffer_capacity(4096)
                            .comment(Some(b'#'))
                            .flexible(true)
                            .has_headers(self.skip_header)
                            .from_reader(BufReader::new(GzReader::from_path(&edge_file).unwrap()));
                        for result in rdr.records() {
                            if let Ok(record) = result {
                                let edge_meta = parser.parse_edge_meta(&record);
                                let (got_src_label, src_lid) = graph
                                    .vertex_map
                                    .get_internal_id(edge_meta.src_global_id)
                                    .unwrap();
                                let (got_dst_label, dst_lid) = graph
                                    .vertex_map
                                    .get_internal_id(edge_meta.dst_global_id)
                                    .unwrap();
                                if got_src_label != src_label || got_dst_label != dst_label {
                                    warn!(
                                        "Edge - {} - {} does not exist",
                                        LDBCVertexParser::<G>::get_original_id(edge_meta.src_global_id)
                                            .index(),
                                        LDBCVertexParser::<G>::get_original_id(edge_meta.dst_global_id)
                                            .index()
                                    );
                                    continue;
                                }
                                if src_delete_set.contains(&src_lid) || dst_delete_set.contains(&dst_lid) {
                                    // warn!("Edge - {} - {} will be removed by vertices", LDBCVertexParser::<G>::get_original_id(edge_meta.src_global_id).index(), LDBCVertexParser::<G>::get_original_id(edge_meta.dst_global_id).index());
                                    continue;
                                }
                                delete_edge_set.push((src_lid, dst_lid));
                            }
                        }
                    } else if edge_file
                        .clone()
                        .to_str()
                        .unwrap()
                        .ends_with(".csv")
                    {
                        let mut rdr = ReaderBuilder::new()
                            .delimiter(self.delim)
                            .buffer_capacity(4096)
                            .comment(Some(b'#'))
                            .flexible(true)
                            .has_headers(self.skip_header)
                            .from_reader(BufReader::new(File::open(&edge_file).unwrap()));
                        for result in rdr.records() {
                            if let Ok(record) = result {
                                let edge_meta = parser.parse_edge_meta(&record);
                                let (got_src_label, src_lid) = graph
                                    .vertex_map
                                    .get_internal_id(edge_meta.src_global_id)
                                    .unwrap();
                                let (got_dst_label, dst_lid) = graph
                                    .vertex_map
                                    .get_internal_id(edge_meta.dst_global_id)
                                    .unwrap();
                                if got_src_label != src_label || got_dst_label == dst_label {
                                    continue;
                                }
                                if src_delete_set.contains(&src_lid) || dst_delete_set.contains(&dst_lid) {
                                    continue;
                                }
                                delete_edge_set.push((src_lid, dst_lid));
                            }
                        }
                    }
                }
            }
        }

        if src_delete_set.is_empty() && dst_delete_set.is_empty() && delete_edge_set.is_empty() {
            return ();
        }
        // println!(
        //     "Deleting edge - {} - {} - {}",
        //     graph.graph_schema.vertex_label_names()[src_label as usize],
        //     graph.graph_schema.edge_label_names()[edge_label as usize],
        //     graph.graph_schema.vertex_label_names()[dst_label as usize]
        // );

        let mut oe_to_delete = Vec::new();
        let mut ie_to_delete = Vec::new();

        for v in src_delete_set.iter() {
            if let Some(oe_list) = input.oe_csr.get_edges(*v) {
                for e in oe_list {
                    if !dst_delete_set.contains(e) {
                        oe_to_delete.push((*v, *e));
                    }
                }
            }
        }
        for v in dst_delete_set.iter() {
            if let Some(ie_list) = input.ie_csr.get_edges(*v) {
                for e in ie_list {
                    if !src_delete_set.contains(e) {
                        ie_to_delete.push((*e, *v));
                    }
                }
            }
        }

        input.oe_csr.delete_vertices(src_delete_set);
        if let Some(table) = input.oe_prop.as_mut() {
            input
                .oe_csr
                .parallel_delete_edges_with_props(&delete_edge_set, false, table, p);
            input
                .oe_csr
                .parallel_delete_edges_with_props(&ie_to_delete, false, table, p);
        } else {
            input
                .oe_csr
                .parallel_delete_edges(&delete_edge_set, false, 4);
            input
                .oe_csr
                .parallel_delete_edges(&ie_to_delete, false, 4);
        }

        input.ie_csr.delete_vertices(dst_delete_set);
        if let Some(table) = input.ie_prop.as_mut() {
            input
                .ie_csr
                .parallel_delete_edges_with_props(&delete_edge_set, true, table, p);
            input
                .ie_csr
                .parallel_delete_edges_with_props(&oe_to_delete, true, table, p);
        } else {
            input
                .ie_csr
                .parallel_delete_edges(&delete_edge_set, true, p);
            input
                .ie_csr
                .parallel_delete_edges(&oe_to_delete, true, p);
        }
    }

    fn apply_deletes<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, delete_schema: &InputSchema,
    ) -> GDBResult<()>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let vertex_label_num = graph.vertex_label_num;
        let edge_label_num = graph.edge_label_num;
        let mut delete_sets = vec![];
        for v_label_i in 0..vertex_label_num {
            let mut delete_set = HashSet::new();
            if let Some(vertex_file_strings) = delete_schema.get_vertex_file(v_label_i as LabelId) {
                if !vertex_file_strings.is_empty() {
                    info!(
                        "Deleting vertex - {}",
                        graph.graph_schema.vertex_label_names()[v_label_i as usize]
                    );
                    let vertex_files_prefix = self.input_dir.clone();
                    let vertex_files = get_files_list_beta(&vertex_files_prefix, &vertex_file_strings);
                    if vertex_files.is_empty() {
                        delete_sets.push(delete_set);
                        continue;
                    }
                    let input_header = delete_schema
                        .get_vertex_header(v_label_i as LabelId)
                        .unwrap();
                    let mut id_col = 0;
                    for (index, (n, _)) in input_header.iter().enumerate() {
                        if n == "id" {
                            id_col = index;
                            break;
                        }
                    }
                    let parser = LDBCVertexParser::<G>::new(v_label_i as LabelId, id_col);
                    for vertex_file in vertex_files.iter() {
                        if vertex_file
                            .clone()
                            .to_str()
                            .unwrap()
                            .ends_with(".csv.gz")
                        {
                            let mut rdr = ReaderBuilder::new()
                                .delimiter(self.delim)
                                .buffer_capacity(4096)
                                .comment(Some(b'#'))
                                .flexible(true)
                                .has_headers(self.skip_header)
                                .from_reader(BufReader::new(GzReader::from_path(&vertex_file).unwrap()));
                            for result in rdr.records() {
                                if let Ok(record) = result {
                                    let vertex_meta = parser.parse_vertex_meta(&record);
                                    let (got_label, lid) = graph
                                        .vertex_map
                                        .get_internal_id(vertex_meta.global_id)
                                        .unwrap();
                                    if got_label == v_label_i as LabelId {
                                        delete_set.insert(lid);
                                    }
                                }
                            }
                        } else if vertex_file
                            .clone()
                            .to_str()
                            .unwrap()
                            .ends_with(".csv")
                        {
                            let mut rdr = ReaderBuilder::new()
                                .delimiter(self.delim)
                                .buffer_capacity(4096)
                                .comment(Some(b'#'))
                                .flexible(true)
                                .has_headers(self.skip_header)
                                .from_reader(BufReader::new(File::open(&vertex_file).unwrap()));
                            for result in rdr.records() {
                                if let Ok(record) = result {
                                    let vertex_meta = parser.parse_vertex_meta(&record);
                                    if let Some((got_label, lid)) = graph
                                        .vertex_map
                                        .get_internal_id(vertex_meta.global_id)
                                    {
                                        if got_label == v_label_i as LabelId {
                                            delete_set.insert(lid);
                                        }
                                    } else {
                                        warn!("parse vertex record: {:?} when delete failed", &record);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            delete_sets.push(delete_set);
        }

        if self.parallel == 1 {
            let mut input_reps = self.take_csrs(graph);
            input_reps
                .par_iter_mut()
                .for_each(|rep| self.delete_rep(rep, &graph, &delete_schema, &delete_sets));
            self.set_csrs(graph, input_reps);
        } else if self.parallel > 1 {
            let mut input_reps = self.take_csrs(graph);
            input_reps.iter_mut().for_each(|rep| {
                self.parallel_delete_rep(rep, &graph, &delete_schema, &delete_sets, self.parallel)
            });
            self.set_csrs(graph, input_reps);
        } else {
            for e_label_i in 0..edge_label_num {
                for src_label_i in 0..vertex_label_num {
                    let src_delete_set = &delete_sets[src_label_i];
                    for dst_label_i in 0..vertex_label_num {
                        let dst_delete_set = &delete_sets[dst_label_i];
                        let mut delete_edge_set = HashSet::new();
                        if graph
                            .graph_schema
                            .get_edge_header(
                                src_label_i as LabelId,
                                e_label_i as LabelId,
                                dst_label_i as LabelId,
                            )
                            .is_none()
                        {
                            continue;
                        }
                        if let Some(edge_file_strings) = delete_schema.get_edge_file(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                        ) {
                            info!(
                                "Deleting edge - {} - {} - {}",
                                graph.graph_schema.vertex_label_names()[src_label_i as usize],
                                graph.graph_schema.edge_label_names()[e_label_i as usize],
                                graph.graph_schema.vertex_label_names()[dst_label_i as usize]
                            );
                            let input_header = delete_schema
                                .get_edge_header(
                                    src_label_i as LabelId,
                                    e_label_i as LabelId,
                                    dst_label_i as LabelId,
                                )
                                .unwrap();

                            let mut src_col_id = 0;
                            let mut dst_col_id = 1;

                            for (index, (n, _)) in input_header.iter().enumerate() {
                                if n == "start_id" {
                                    src_col_id = index;
                                }
                                if n == "end_id" {
                                    dst_col_id = index;
                                }
                            }

                            let mut parser = LDBCEdgeParser::<G>::new(
                                src_label_i as LabelId,
                                dst_label_i as LabelId,
                                e_label_i as LabelId,
                            );
                            parser.with_endpoint_col_id(src_col_id, dst_col_id);

                            let edge_files_prefix = self.input_dir.clone();
                            let edge_files = get_files_list(&edge_files_prefix, &edge_file_strings);
                            if edge_files.is_err() {
                                warn!(
                                    "Get edge files {:?}/{:?} failed: {:?}",
                                    &edge_files_prefix,
                                    &edge_file_strings,
                                    edge_files.err().unwrap()
                                );
                                continue;
                            }
                            let edge_files = edge_files.unwrap();
                            if edge_files.is_empty() {
                                continue;
                            }

                            for edge_file in edge_files.iter() {
                                if edge_file
                                    .clone()
                                    .to_str()
                                    .unwrap()
                                    .ends_with(".csv.gz")
                                {
                                    let mut rdr = ReaderBuilder::new()
                                        .delimiter(self.delim)
                                        .buffer_capacity(4096)
                                        .comment(Some(b'#'))
                                        .flexible(true)
                                        .has_headers(self.skip_header)
                                        .from_reader(BufReader::new(
                                            GzReader::from_path(&edge_file).unwrap(),
                                        ));
                                    for result in rdr.records() {
                                        if let Ok(record) = result {
                                            let edge_meta = parser.parse_edge_meta(&record);
                                            let (got_src_label, src_lid) = graph
                                                .vertex_map
                                                .get_internal_id(edge_meta.src_global_id)
                                                .unwrap();
                                            let (got_dst_label, dst_lid) = graph
                                                .vertex_map
                                                .get_internal_id(edge_meta.dst_global_id)
                                                .unwrap();
                                            if got_src_label != src_label_i as LabelId
                                                || got_dst_label != dst_label_i as LabelId
                                            {
                                                warn!(
                                                    "Edge - {} - {} does not exist",
                                                    LDBCVertexParser::<G>::get_original_id(
                                                        edge_meta.src_global_id
                                                    )
                                                    .index(),
                                                    LDBCVertexParser::<G>::get_original_id(
                                                        edge_meta.dst_global_id
                                                    )
                                                    .index()
                                                );
                                                continue;
                                            }
                                            if src_delete_set.contains(&src_lid)
                                                || dst_delete_set.contains(&dst_lid)
                                            {
                                                // warn!("Edge - {} - {} will be removed by vertices", LDBCVertexParser::<G>::get_original_id(edge_meta.src_global_id).index(), LDBCVertexParser::<G>::get_original_id(edge_meta.dst_global_id).index());
                                                continue;
                                            }
                                            delete_edge_set.insert((src_lid, dst_lid));
                                        }
                                    }
                                } else if edge_file
                                    .clone()
                                    .to_str()
                                    .unwrap()
                                    .ends_with(".csv")
                                {
                                    let mut rdr = ReaderBuilder::new()
                                        .delimiter(self.delim)
                                        .buffer_capacity(4096)
                                        .comment(Some(b'#'))
                                        .flexible(true)
                                        .has_headers(self.skip_header)
                                        .from_reader(BufReader::new(File::open(&edge_file).unwrap()));
                                    for result in rdr.records() {
                                        if let Ok(record) = result {
                                            let edge_meta = parser.parse_edge_meta(&record);
                                            let (got_src_label, src_lid) = graph
                                                .vertex_map
                                                .get_internal_id(edge_meta.src_global_id)
                                                .unwrap();
                                            let (got_dst_label, dst_lid) = graph
                                                .vertex_map
                                                .get_internal_id(edge_meta.dst_global_id)
                                                .unwrap();
                                            if got_src_label != src_label_i as LabelId
                                                || got_dst_label == dst_label_i as LabelId
                                            {
                                                continue;
                                            }
                                            if src_delete_set.contains(&src_lid)
                                                || dst_delete_set.contains(&dst_lid)
                                            {
                                                continue;
                                            }
                                            delete_edge_set.insert((src_lid, dst_lid));
                                        }
                                    }
                                }
                            }
                        }

                        if src_delete_set.is_empty()
                            && dst_delete_set.is_empty()
                            && delete_edge_set.is_empty()
                        {
                            continue;
                        }
                        graph.delete_edges(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                            src_delete_set,
                            dst_delete_set,
                            &delete_edge_set,
                        );
                    }
                }
            }
        }

        for v_label_i in 0..vertex_label_num {
            let delete_set = &delete_sets[v_label_i as usize];
            if delete_set.is_empty() {
                continue;
            }
            for v in delete_set.iter() {
                graph
                    .vertex_map
                    .remove_vertex(v_label_i as LabelId, v);
            }
        }

        Ok(())
    }

    fn apply_vertices_inserts<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema,
    ) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let v_label_num = graph.vertex_label_num;
        for v_label_i in 0..v_label_num {
            if let Some(vertex_file_strings) = input_schema.get_vertex_file(v_label_i as LabelId) {
                if vertex_file_strings.is_empty() {
                    continue;
                }

                let input_header = input_schema
                    .get_vertex_header(v_label_i as LabelId)
                    .unwrap();
                let graph_header = graph
                    .graph_schema
                    .get_vertex_header(v_label_i as LabelId)
                    .unwrap();
                let mut keep_set = HashSet::new();
                for pair in graph_header {
                    keep_set.insert(pair.0.clone());
                }
                let mut selected = vec![false; input_header.len()];
                let mut id_col_id = 0;
                for (index, (n, _)) in input_header.iter().enumerate() {
                    if keep_set.contains(n) {
                        selected[index] = true;
                    }
                    if n == "id" {
                        id_col_id = index;
                    }
                }
                let parser = LDBCVertexParser::<G>::new(v_label_i as LabelId, id_col_id);
                let vertex_files_prefix = self.input_dir.clone();

                let vertex_files = get_files_list(&vertex_files_prefix, &vertex_file_strings);
                if vertex_files.is_err() {
                    warn!(
                        "Get vertex files {:?}/{:?} failed: {:?}",
                        &vertex_files_prefix,
                        &vertex_file_strings,
                        vertex_files.err().unwrap()
                    );
                    continue;
                }
                let vertex_files = vertex_files.unwrap();
                if vertex_files.is_empty() {
                    continue;
                }
                for vertex_file in vertex_files.iter() {
                    if vertex_file
                        .clone()
                        .to_str()
                        .unwrap()
                        .ends_with(".csv.gz")
                    {
                        let mut rdr = ReaderBuilder::new()
                            .delimiter(self.delim)
                            .buffer_capacity(4096)
                            .comment(Some(b'#'))
                            .flexible(true)
                            .has_headers(self.skip_header)
                            .from_reader(BufReader::new(GzReader::from_path(&vertex_file).unwrap()));
                        for result in rdr.records() {
                            if let Ok(record) = result {
                                let vertex_meta = parser.parse_vertex_meta(&record);
                                if let Ok(properties) =
                                    parse_properties(&record, input_header, selected.as_slice())
                                {
                                    graph.insert_vertex(
                                        vertex_meta.label,
                                        vertex_meta.global_id,
                                        Some(properties),
                                    );
                                }
                            }
                        }
                    } else if vertex_file
                        .clone()
                        .to_str()
                        .unwrap()
                        .ends_with(".csv")
                    {
                        let mut rdr = ReaderBuilder::new()
                            .delimiter(self.delim)
                            .buffer_capacity(4096)
                            .comment(Some(b'#'))
                            .flexible(true)
                            .has_headers(self.skip_header)
                            .from_reader(BufReader::new(File::open(&vertex_file).unwrap()));
                        for result in rdr.records() {
                            if let Ok(record) = result {
                                let vertex_meta = parser.parse_vertex_meta(&record);
                                if let Ok(properties) =
                                    parse_properties(&record, input_header, selected.as_slice())
                                {
                                    graph.insert_vertex(
                                        vertex_meta.label,
                                        vertex_meta.global_id,
                                        Some(properties),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn load_insert_edges_with_no_prop<G>(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, input_schema: &InputSchema,
        graph_schema: &CsrGraphSchema, files: &Vec<PathBuf>,
    ) -> GDBResult<(Vec<(G, G)>, Option<ColTable>)>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let mut edges = vec![];

        let input_header = input_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let graph_header = graph_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let mut keep_set = HashSet::new();
        for pair in graph_header {
            keep_set.insert(pair.0.clone());
        }
        let mut selected = vec![false; input_header.len()];
        let mut src_col_id = 0;
        let mut dst_col_id = 1;
        for (index, (n, _)) in input_header.iter().enumerate() {
            if keep_set.contains(n) {
                selected[index] = true;
            }
            if n == "start_id" {
                src_col_id = index;
            }
            if n == "end_id" {
                dst_col_id = index;
            }
        }
        let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
        parser.with_endpoint_col_id(src_col_id, dst_col_id);

        for file in files.iter() {
            if file
                .clone()
                .to_str()
                .unwrap()
                .ends_with(".csv.gz")
            {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(self.delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(GzReader::from_path(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let edge_meta = parser.parse_edge_meta(&record);
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                    }
                }
            } else if file.clone().to_str().unwrap().ends_with(".csv") {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(self.delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(File::open(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let edge_meta = parser.parse_edge_meta(&record);
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                    }
                }
            }
        }

        Ok((edges, None))
    }

    fn load_insert_edges_with_prop<G>(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, input_schema: &InputSchema,
        graph_schema: &CsrGraphSchema, files: &Vec<PathBuf>,
    ) -> GDBResult<(Vec<(G, G)>, Option<ColTable>)>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let mut edges = vec![];

        let input_header = input_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let graph_header = graph_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let mut table_header = vec![];
        let mut keep_set = HashSet::new();
        for pair in graph_header {
            table_header.push((pair.1.clone(), pair.0.clone()));
            keep_set.insert(pair.0.clone());
        }
        let mut prop_table = ColTable::new(table_header);

        let mut selected = vec![false; input_header.len()];
        let mut src_col_id = 0;
        let mut dst_col_id = 1;
        for (index, (n, _)) in input_header.iter().enumerate() {
            if keep_set.contains(n) {
                selected[index] = true;
            }
            if n == "start_id" {
                src_col_id = index;
            }
            if n == "end_id" {
                dst_col_id = index;
            }
        }

        let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
        parser.with_endpoint_col_id(src_col_id, dst_col_id);

        for file in files.iter() {
            if file
                .clone()
                .to_str()
                .unwrap()
                .ends_with(".csv.gz")
            {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(self.delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(GzReader::from_path(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let edge_meta = parser.parse_edge_meta(&record);
                        let properties =
                            parse_properties(&record, input_header, selected.as_slice()).unwrap();
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                        prop_table.push(&properties);
                    }
                }
            } else if file.clone().to_str().unwrap().ends_with(".csv") {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(self.delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(File::open(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let edge_meta = parser.parse_edge_meta(&record);
                        let properties =
                            parse_properties(&record, input_header, selected.as_slice()).unwrap();
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                        prop_table.push(&properties);
                    }
                }
            }
        }

        Ok((edges, Some(prop_table)))
    }

    fn insert_rep<G, I>(
        &self, mut input: CsrRep<I>, graph: &GraphDB<G, I>, input_schema: &InputSchema,
    ) -> CsrRep<I>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let t = Instant::now();
        let src_label = input.src_label;
        let edge_label = input.edge_label;
        let dst_label = input.dst_label;

        let graph_header = graph
            .graph_schema
            .get_edge_header(src_label, edge_label, dst_label);
        if graph_header.is_none() {
            return input;
        }
        let graph_header = graph_header.unwrap();

        let edge_file_strings = input_schema.get_edge_file(src_label, edge_label, dst_label);
        if edge_file_strings.is_none() {
            return input;
        }
        let edge_file_strings = edge_file_strings.unwrap();
        if edge_file_strings.is_empty() {
            return input;
        }

        let edge_files = get_files_list(&self.input_dir.clone(), edge_file_strings);
        if edge_files.is_err() {
            return input;
        }
        let edge_files = edge_files.unwrap();
        if edge_files.is_empty() {
            return input;
        }

        let (edges, table) = if graph_header.len() > 0 {
            self.load_insert_edges_with_prop::<G>(
                src_label,
                edge_label,
                dst_label,
                input_schema,
                &graph.graph_schema,
                &edge_files,
            )
            .unwrap()
        } else {
            self.load_insert_edges_with_no_prop::<G>(
                src_label,
                edge_label,
                dst_label,
                input_schema,
                &graph.graph_schema,
                &edge_files,
            )
            .unwrap()
        };

        let mut parsed_edges = vec![];
        for (src, dst) in edges {
            let (got_src_label, src_lid) = graph.vertex_map.get_internal_id(src).unwrap();
            let (got_dst_label, dst_lid) = graph.vertex_map.get_internal_id(dst).unwrap();
            if got_src_label != src_label || got_dst_label != dst_label {
                warn!("insert edges with wrong label");
                parsed_edges.push((<I as IndexType>::max(), <I as IndexType>::max()));
                continue;
            }
            parsed_edges.push((src_lid, dst_lid));
        }

        let oe_single = graph
            .graph_schema
            .is_single_oe(src_label, edge_label, dst_label);
        let ie_single = graph
            .graph_schema
            .is_single_ie(src_label, edge_label, dst_label);

        let (new_oe_csr, new_oe_table) = if oe_single {
            let casted_oe = input
                .oe_csr
                .as_mut_any()
                .downcast_mut::<BatchMutableSingleCsr<I>>()
                .unwrap();
            casted_oe.resize_vertex(graph.vertex_map.vertex_num(src_label));
            if input.oe_prop.is_none() {
                for (src, dst) in parsed_edges.iter() {
                    casted_oe.insert_edge(*src, *dst);
                }
                (input.oe_csr, None)
            } else {
                let new_table = table.as_ref().unwrap();
                let mut oe_prop = input.oe_prop.unwrap();
                for (index, (src, dst)) in parsed_edges.iter().enumerate() {
                    casted_oe.insert_edge(*src, *dst);
                    let row = new_table.get_row(index).unwrap();
                    oe_prop.insert(src.index(), &row);
                }
                (input.oe_csr, Some(oe_prop))
            }
        } else {
            let new_vertex_num = graph.vertex_map.vertex_num(src_label);
            let mut new_degree = vec![0 as i32; new_vertex_num];
            let old_vertex_num = input.oe_csr.vertex_num();
            for v in 0..old_vertex_num.index() {
                new_degree[v] += input.oe_csr.degree(I::new(v)) as i32;
            }

            let casted_oe = input
                .oe_csr
                .as_mut_any()
                .downcast_mut::<BatchMutableCsr<I>>()
                .unwrap();

            let mut builder = BatchMutableCsrBuilder::<I>::new();
            for (src, _) in parsed_edges.iter() {
                new_degree[src.index()] += 1;
            }

            builder.init(&new_degree, 1.0);

            if let Some(input_table) = table.as_ref() {
                let mut new_table = ColTable::new({
                    let cols = graph
                        .graph_schema
                        .get_edge_header(src_label, edge_label, dst_label)
                        .unwrap();
                    let mut header = vec![];
                    for pair in cols {
                        header.push((pair.1.clone(), pair.0.clone()));
                    }
                    header
                });

                let old_table = input.oe_prop.unwrap();
                for v in 0..old_vertex_num.index() {
                    for (nbr, offset) in casted_oe
                        .get_edges_with_offset(I::new(v))
                        .unwrap()
                    {
                        let new_offset = builder.put_edge(I::new(v), nbr).unwrap();
                        let row = old_table.get_row(offset).unwrap();
                        new_table.insert(new_offset, &row);
                    }
                }
                for (index, (src, dst)) in parsed_edges.iter().enumerate() {
                    let new_offset = builder.put_edge(*src, *dst).unwrap();
                    let row = input_table.get_row(index).unwrap();
                    new_table.insert(new_offset, &row);
                }
                let new_oe: Box<dyn CsrTrait<I>> = Box::new(builder.finish().unwrap());
                (new_oe, Some(new_table))
            } else {
                for v in 0..old_vertex_num.index() {
                    for nbr in casted_oe.get_edges(I::new(v)).unwrap() {
                        builder.put_edge(I::new(v), *nbr).unwrap();
                    }
                }
                for (src, dst) in parsed_edges.iter() {
                    builder.put_edge(*src, *dst).unwrap();
                }

                let new_oe: Box<dyn CsrTrait<I>> = Box::new(builder.finish().unwrap());
                (new_oe, None)
            }
        };

        let (new_ie_csr, new_ie_table) = if ie_single {
            let casted_ie = input
                .ie_csr
                .as_mut_any()
                .downcast_mut::<BatchMutableSingleCsr<I>>()
                .unwrap();
            casted_ie.resize_vertex(graph.vertex_map.vertex_num(dst_label));
            if input.ie_prop.is_none() {
                for (src, dst) in parsed_edges.iter() {
                    casted_ie.insert_edge(*dst, *src);
                }
                (input.ie_csr, None)
            } else {
                let new_table = table.as_ref().unwrap();
                let mut ie_prop = input.ie_prop.unwrap();
                for (index, (src, dst)) in parsed_edges.iter().enumerate() {
                    casted_ie.insert_edge(*dst, *src);
                    let row = new_table.get_row(index).unwrap();
                    ie_prop.insert(dst.index(), &row);
                }
                (input.ie_csr, Some(ie_prop))
            }
        } else {
            let new_vertex_num = graph.vertex_map.vertex_num(dst_label);
            let mut new_degree = vec![0 as i32; new_vertex_num];
            let old_vertex_num = input.ie_csr.vertex_num();
            for v in 0..old_vertex_num.index() {
                new_degree[v] += input.ie_csr.degree(I::new(v)) as i32;
            }

            let casted_ie = input
                .ie_csr
                .as_mut_any()
                .downcast_mut::<BatchMutableCsr<I>>()
                .unwrap();

            let mut builder = BatchMutableCsrBuilder::<I>::new();
            for (_, dst) in parsed_edges.iter() {
                new_degree[dst.index()] += 1;
            }

            builder.init(&new_degree, 1.0);

            if let Some(input_table) = table.as_ref() {
                let mut new_table = ColTable::new({
                    let cols = graph
                        .graph_schema
                        .get_edge_header(src_label, edge_label, dst_label)
                        .unwrap();
                    let mut header = vec![];
                    for pair in cols {
                        header.push((pair.1.clone(), pair.0.clone()));
                    }
                    header
                });

                let old_table = input.ie_prop.unwrap();
                for v in 0..old_vertex_num.index() {
                    for (nbr, offset) in casted_ie
                        .get_edges_with_offset(I::new(v))
                        .unwrap()
                    {
                        let new_offset = builder.put_edge(I::new(v), nbr).unwrap();
                        let row = old_table.get_row(offset).unwrap();
                        new_table.insert(new_offset, &row);
                    }
                }
                for (index, (src, dst)) in parsed_edges.iter().enumerate() {
                    let new_offset = builder.put_edge(*dst, *src).unwrap();
                    let row = input_table.get_row(index).unwrap();
                    new_table.insert(new_offset, &row);
                }
                let new_ie: Box<dyn CsrTrait<I>> = Box::new(builder.finish().unwrap());
                (new_ie, Some(new_table))
            } else {
                for v in 0..old_vertex_num.index() {
                    for nbr in casted_ie.get_edges(I::new(v)).unwrap() {
                        builder.put_edge(I::new(v), *nbr).unwrap();
                    }
                }
                for (src, dst) in parsed_edges.iter() {
                    builder.put_edge(*dst, *src).unwrap();
                }

                let new_ie: Box<dyn CsrTrait<I>> = Box::new(builder.finish().unwrap());
                (new_ie, None)
            }
        };

        println!(
            "insert edge: {} - {} - {}: {}",
            graph.graph_schema.vertex_label_names()[src_label as usize],
            graph.graph_schema.edge_label_names()[edge_label as usize],
            graph.graph_schema.vertex_label_names()[dst_label as usize],
            t.elapsed().as_secs_f32(),
        );
        CsrRep {
            src_label: src_label,
            edge_label: edge_label,
            dst_label: dst_label,
            ie_csr: new_ie_csr,
            ie_prop: new_ie_table,
            oe_csr: new_oe_csr,
            oe_prop: new_oe_table,
        }
    }

    fn parallel_insert_rep<G, I>(
        &self, mut input: CsrRep<I>, graph: &GraphDB<G, I>, input_schema: &InputSchema, p: u32,
    ) -> CsrRep<I>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let t = Instant::now();
        let src_label = input.src_label;
        let edge_label = input.edge_label;
        let dst_label = input.dst_label;

        let graph_header = graph
            .graph_schema
            .get_edge_header(src_label, edge_label, dst_label);
        if graph_header.is_none() {
            return input;
        }
        let graph_header = graph_header.unwrap();

        let edge_file_strings = input_schema.get_edge_file(src_label, edge_label, dst_label);
        if edge_file_strings.is_none() {
            return input;
        }
        let edge_file_strings = edge_file_strings.unwrap();
        if edge_file_strings.is_empty() {
            return input;
        }

        let edge_files = get_files_list(&self.input_dir.clone(), edge_file_strings);
        if edge_files.is_err() {
            return input;
        }
        let edge_files = edge_files.unwrap();
        if edge_files.is_empty() {
            return input;
        }

        let load_t = Instant::now();
        let (edges, table) = if graph_header.len() > 0 {
            self.load_insert_edges_with_prop::<G>(
                src_label,
                edge_label,
                dst_label,
                input_schema,
                &graph.graph_schema,
                &edge_files,
            )
            .unwrap()
        } else {
            self.load_insert_edges_with_no_prop::<G>(
                src_label,
                edge_label,
                dst_label,
                input_schema,
                &graph.graph_schema,
                &edge_files,
            )
            .unwrap()
        };
        let load_t = load_t.elapsed().as_secs_f32();

        let parse_t = Instant::now();
        // let mut parsed_edges = vec![];
        // for (src, dst) in edges {
        //     let (got_src_label, src_lid) = graph.vertex_map.get_internal_id(src).unwrap();
        //     let (got_dst_label, dst_lid) = graph.vertex_map.get_internal_id(dst).unwrap();
        //     if got_src_label != src_label || got_dst_label != dst_label {
        //         warn!("insert edges with wrong label");
        //         parsed_edges.push((<I as IndexType>::max(), <I as IndexType>::max()));
        //         continue;
        //     }
        //     parsed_edges.push((src_lid, dst_lid));
        // }
        let parsed_edges: Vec<(I, I)> = edges
            .par_iter()
            .map(|(src, dst)| {
                let (got_src_label, src_lid) = graph.vertex_map.get_internal_id(*src).unwrap();
                let (got_dst_label, dst_lid) = graph.vertex_map.get_internal_id(*dst).unwrap();
                if got_src_label != src_label || got_dst_label != dst_label {
                    warn!("insert edges with wrong label");
                    (<I as IndexType>::max(), <I as IndexType>::max())
                } else {
                    (src_lid, dst_lid)
                }
            })
            .collect();

        let parse_t = parse_t.elapsed().as_secs_f32();

        let insert_t = Instant::now();
        let oe_single = graph
            .graph_schema
            .is_single_oe(src_label, edge_label, dst_label);
        let ie_single = graph
            .graph_schema
            .is_single_ie(src_label, edge_label, dst_label);

        let (new_oe_csr, new_oe_table) = if oe_single {
            let casted_oe = input
                .oe_csr
                .as_mut_any()
                .downcast_mut::<BatchMutableSingleCsr<I>>()
                .unwrap();
            if input.oe_prop.is_none() {
                // casted_oe.resize_vertex(graph.vertex_map.vertex_num(src_label));
                // for (src, dst) in parsed_edges.iter() {
                //     casted_oe.insert_edge(*src, *dst);
                // }
                casted_oe.insert_edges(graph.vertex_map.vertex_num(src_label), &parsed_edges, false, p);
                (input.oe_csr, None)
            } else {
                println!("oe single no parallel");
                casted_oe.resize_vertex(graph.vertex_map.vertex_num(src_label));
                let new_table = table.as_ref().unwrap();
                let mut oe_prop = input.oe_prop.unwrap();
                for (index, (src, dst)) in parsed_edges.iter().enumerate() {
                    casted_oe.insert_edge(*src, *dst);
                    let row = new_table.get_row(index).unwrap();
                    oe_prop.insert(src.index(), &row);
                }
                (input.oe_csr, Some(oe_prop))
            }
        } else {
            let new_vertex_num = graph.vertex_map.vertex_num(src_label);
            if table.is_none() {
                // if false {
                let casted_oe = input
                    .oe_csr
                    .as_mut_any()
                    .downcast_mut::<BatchMutableCsr<I>>()
                    .unwrap();
                let new_oe: Box<dyn CsrTrait<I>> =
                    Box::new(casted_oe.insert_edges(new_vertex_num, &parsed_edges, false, p));
                (new_oe, None)
            } else {
                println!("oe no parallel");
                let mut new_degree = vec![0 as i32; new_vertex_num];
                let old_vertex_num = input.oe_csr.vertex_num();
                for v in 0..old_vertex_num.index() {
                    new_degree[v] += input.oe_csr.degree(I::new(v)) as i32;
                }

                let casted_oe = input
                    .oe_csr
                    .as_mut_any()
                    .downcast_mut::<BatchMutableCsr<I>>()
                    .unwrap();

                let mut builder = BatchMutableCsrBuilder::<I>::new();
                for (src, _) in parsed_edges.iter() {
                    new_degree[src.index()] += 1;
                }

                builder.init(&new_degree, 1.0);

                if let Some(input_table) = table.as_ref() {
                    let mut new_table = ColTable::new({
                        let cols = graph
                            .graph_schema
                            .get_edge_header(src_label, edge_label, dst_label)
                            .unwrap();
                        let mut header = vec![];
                        for pair in cols {
                            header.push((pair.1.clone(), pair.0.clone()));
                        }
                        header
                    });

                    let old_table = input.oe_prop.unwrap();
                    for v in 0..old_vertex_num.index() {
                        for (nbr, offset) in casted_oe
                            .get_edges_with_offset(I::new(v))
                            .unwrap()
                        {
                            let new_offset = builder.put_edge(I::new(v), nbr).unwrap();
                            let row = old_table.get_row(offset).unwrap();
                            new_table.insert(new_offset, &row);
                        }
                    }
                    for (index, (src, dst)) in parsed_edges.iter().enumerate() {
                        let new_offset = builder.put_edge(*src, *dst).unwrap();
                        let row = input_table.get_row(index).unwrap();
                        new_table.insert(new_offset, &row);
                    }
                    let new_oe: Box<dyn CsrTrait<I>> = Box::new(builder.finish().unwrap());
                    (new_oe, Some(new_table))
                } else {
                    for v in 0..old_vertex_num.index() {
                        for nbr in casted_oe.get_edges(I::new(v)).unwrap() {
                            builder.put_edge(I::new(v), *nbr).unwrap();
                        }
                    }
                    for (src, dst) in parsed_edges.iter() {
                        builder.put_edge(*src, *dst).unwrap();
                    }

                    let new_oe: Box<dyn CsrTrait<I>> = Box::new(builder.finish().unwrap());
                    (new_oe, None)
                }
            }
        };

        let (new_ie_csr, new_ie_table) = if ie_single {
            let casted_ie = input
                .ie_csr
                .as_mut_any()
                .downcast_mut::<BatchMutableSingleCsr<I>>()
                .unwrap();
            if input.ie_prop.is_none() {
                // casted_ie.resize_vertex(graph.vertex_map.vertex_num(dst_label));
                // for (src, dst) in parsed_edges.iter() {
                //     casted_ie.insert_edge(*dst, *src);
                // }
                casted_ie.insert_edges(graph.vertex_map.vertex_num(dst_label), &parsed_edges, true, p);
                (input.ie_csr, None)
            } else {
                println!("ie single no parallel");
                casted_ie.resize_vertex(graph.vertex_map.vertex_num(dst_label));
                let new_table = table.as_ref().unwrap();
                let mut ie_prop = input.ie_prop.unwrap();
                for (index, (src, dst)) in parsed_edges.iter().enumerate() {
                    casted_ie.insert_edge(*dst, *src);
                    let row = new_table.get_row(index).unwrap();
                    ie_prop.insert(dst.index(), &row);
                }
                (input.ie_csr, Some(ie_prop))
            }
        } else {
            let new_vertex_num = graph.vertex_map.vertex_num(dst_label);
            if table.is_none() {
                // if false {
                let casted_ie = input
                    .ie_csr
                    .as_mut_any()
                    .downcast_mut::<BatchMutableCsr<I>>()
                    .unwrap();
                let new_ie: Box<dyn CsrTrait<I>> =
                    Box::new(casted_ie.insert_edges(new_vertex_num, &parsed_edges, true, p));
                (new_ie, None)
            } else {
                println!("ie no parallel");
                let mut new_degree = vec![0 as i32; new_vertex_num];
                let old_vertex_num = input.ie_csr.vertex_num();
                for v in 0..old_vertex_num.index() {
                    new_degree[v] += input.ie_csr.degree(I::new(v)) as i32;
                }

                let casted_ie = input
                    .ie_csr
                    .as_mut_any()
                    .downcast_mut::<BatchMutableCsr<I>>()
                    .unwrap();

                let mut builder = BatchMutableCsrBuilder::<I>::new();
                for (_, dst) in parsed_edges.iter() {
                    new_degree[dst.index()] += 1;
                }

                builder.init(&new_degree, 1.0);

                if let Some(input_table) = table.as_ref() {
                    let mut new_table = ColTable::new({
                        let cols = graph
                            .graph_schema
                            .get_edge_header(src_label, edge_label, dst_label)
                            .unwrap();
                        let mut header = vec![];
                        for pair in cols {
                            header.push((pair.1.clone(), pair.0.clone()));
                        }
                        header
                    });

                    let old_table = input.ie_prop.unwrap();
                    for v in 0..old_vertex_num.index() {
                        for (nbr, offset) in casted_ie
                            .get_edges_with_offset(I::new(v))
                            .unwrap()
                        {
                            let new_offset = builder.put_edge(I::new(v), nbr).unwrap();
                            let row = old_table.get_row(offset).unwrap();
                            new_table.insert(new_offset, &row);
                        }
                    }
                    for (index, (src, dst)) in parsed_edges.iter().enumerate() {
                        let new_offset = builder.put_edge(*dst, *src).unwrap();
                        let row = input_table.get_row(index).unwrap();
                        new_table.insert(new_offset, &row);
                    }
                    let new_ie: Box<dyn CsrTrait<I>> = Box::new(builder.finish().unwrap());
                    (new_ie, Some(new_table))
                } else {
                    for v in 0..old_vertex_num.index() {
                        for nbr in casted_ie.get_edges(I::new(v)).unwrap() {
                            builder.put_edge(I::new(v), *nbr).unwrap();
                        }
                    }
                    for (src, dst) in parsed_edges.iter() {
                        builder.put_edge(*dst, *src).unwrap();
                    }

                    let new_ie: Box<dyn CsrTrait<I>> = Box::new(builder.finish().unwrap());
                    (new_ie, None)
                }
            }
        };
        let insert_t = insert_t.elapsed().as_secs_f32();
        println!(
            "insert edge (parallel{}): {} - {} - {}: {}",
            p,
            graph.graph_schema.vertex_label_names()[src_label as usize],
            graph.graph_schema.edge_label_names()[edge_label as usize],
            graph.graph_schema.vertex_label_names()[dst_label as usize],
            t.elapsed().as_secs_f32(),
        );
        println!("\tload: {}", load_t);
        println!("\tparse: {}", parse_t);
        println!("\tinsert: {}", insert_t);

        CsrRep {
            src_label: src_label,
            edge_label: edge_label,
            dst_label: dst_label,
            ie_csr: new_ie_csr,
            ie_prop: new_ie_table,
            oe_csr: new_oe_csr,
            oe_prop: new_oe_table,
        }
    }

    fn apply_edges_inserts<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema,
    ) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        if self.parallel == 1 {
            let input_reps = self.take_csrs(graph);
            let output_reps = input_reps
                .into_par_iter()
                .map(|input| self.insert_rep(input, graph, input_schema))
                .collect();
            self.set_csrs(graph, output_reps);
        } else if self.parallel > 1 {
            let input_reps = self.take_csrs(graph);
            let output_reps = input_reps
                .into_iter()
                .map(|input| self.parallel_insert_rep(input, graph, input_schema, self.parallel))
                .collect();
            self.set_csrs(graph, output_reps);
        } else {
            let vertex_label_num = graph.vertex_label_num;
            let edge_label_num = graph.edge_label_num;

            for e_label_i in 0..edge_label_num {
                for src_label_i in 0..vertex_label_num {
                    for dst_label_i in 0..vertex_label_num {
                        if let Some(edge_file_strings) = input_schema.get_edge_file(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                        ) {
                            if edge_file_strings.is_empty() {
                                continue;
                            }
                            let t = Instant::now();
                            let graph_header = graph
                                .graph_schema
                                .get_edge_header(
                                    src_label_i as LabelId,
                                    e_label_i as LabelId,
                                    dst_label_i as LabelId,
                                )
                                .unwrap();
                            let edge_files_prefix = self.input_dir.clone();
                            let edge_files = get_files_list(&edge_files_prefix, &edge_file_strings);
                            if edge_files.is_err() {
                                warn!(
                                    "Get edge files {:?}/{:?} failed: {:?}",
                                    &edge_files_prefix,
                                    &edge_file_strings,
                                    edge_files.err().unwrap()
                                );
                                continue;
                            }
                            let edge_files = edge_files.unwrap();
                            if edge_files.is_empty() {
                                continue;
                            }
                            let (edges, table) = if graph_header.len() > 0 {
                                info!(
                                    "Loading edges with properties: {}, {}, {}",
                                    graph.graph_schema.vertex_label_names()[src_label_i as usize],
                                    graph.graph_schema.edge_label_names()[e_label_i as usize],
                                    graph.graph_schema.vertex_label_names()[dst_label_i as usize]
                                );
                                self.load_insert_edges_with_prop(
                                    src_label_i as LabelId,
                                    e_label_i as LabelId,
                                    dst_label_i as LabelId,
                                    input_schema,
                                    &graph.graph_schema,
                                    &edge_files,
                                )
                                .unwrap()
                            } else {
                                info!(
                                    "Loading edges with no property: {}, {}, {}",
                                    graph.graph_schema.vertex_label_names()[src_label_i as usize],
                                    graph.graph_schema.edge_label_names()[e_label_i as usize],
                                    graph.graph_schema.vertex_label_names()[dst_label_i as usize]
                                );
                                self.load_insert_edges_with_no_prop(
                                    src_label_i as LabelId,
                                    e_label_i as LabelId,
                                    dst_label_i as LabelId,
                                    input_schema,
                                    &graph.graph_schema,
                                    &edge_files,
                                )
                                .unwrap()
                            };

                            graph.insert_edges(
                                src_label_i as LabelId,
                                e_label_i as LabelId,
                                dst_label_i as LabelId,
                                edges,
                                table,
                            );
                            println!(
                                "insert edge: {} - {} - {}: {}",
                                graph.graph_schema.vertex_label_names()[src_label_i],
                                graph.graph_schema.edge_label_names()[e_label_i],
                                graph.graph_schema.vertex_label_names()[dst_label_i],
                                t.elapsed().as_secs_f32(),
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn insert<G, I>(&mut self, graph: &mut GraphDB<G, I>, insert_schema: &InputSchema) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        self.apply_vertices_inserts(graph, &insert_schema)?;
        self.apply_edges_inserts(graph, &insert_schema)?;
        Ok(())
    }

    pub fn delete<G, I>(&mut self, graph: &mut GraphDB<G, I>, delete_schema: &InputSchema) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        self.apply_deletes(graph, &delete_schema)?;
        Ok(())
    }
}
