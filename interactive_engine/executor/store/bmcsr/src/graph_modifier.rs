use std::collections::HashSet;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use csv::ReaderBuilder;
use rust_htslib::bgzf::Reader as GzReader;
use crate::col_table::parse_properties;
use crate::columns::{Column, StringColumn};

use crate::error::GDBResult;
use crate::graph::{Direction, IndexType};
use crate::graph_db::GraphDB;
use crate::graph_loader::get_files_list;
use crate::ldbc_parser::{LDBCEdgeParser, LDBCVertexParser};
use crate::schema::{CsrGraphSchema, InputSchema, Schema};
use crate::sub_graph::SubGraph;
use crate::types::{DefaultId, InternalId, LabelId};

pub struct GraphModifier<G: FromStr + Send + Sync + IndexType = DefaultId> {
    input_dir: PathBuf,

    work_id: usize,
    peers: usize,
    delim: u8,
    graph_schema: Arc<CsrGraphSchema>,
    skip_header: bool,

    persons_to_delete: Vec<G>,
    forums_to_delete: Vec<G>,
    posts_to_delete: Vec<G>,
    comments_to_delete: Vec<G>,
}

impl<G: FromStr + Send + Sync + IndexType + Eq> GraphModifier<G> {
    pub fn new<D: AsRef<Path>>(
        input_dir: D, graph_schema_file: D, work_id: usize,
        peers: usize,
    ) -> GraphModifier<G> {
        let graph_schema =
            CsrGraphSchema::from_json_file(graph_schema_file).expect("Read trim schema error!");
        graph_schema.desc();

        Self {
            input_dir: input_dir.as_ref().to_path_buf(),

            work_id,
            peers,
            delim: b'|',
            graph_schema: Arc::new(graph_schema),
            skip_header: false,

            persons_to_delete: vec![],
            forums_to_delete: vec![],
            posts_to_delete: vec![],
            comments_to_delete: vec![],
        }
    }

    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn skip_header(&mut self) {
        self.skip_header = true;
    }

    fn load_vertices_to_delete(&self, label_name: &str, batch: &str) ->GDBResult<Vec<G>> {
        let mut results = vec![];
        let files_list_prefix = "/deletes/dynamic/".to_string() + label_name + "/batch_id=" + batch;
        let files_list_input = vec![
            (files_list_prefix.clone() + "/*.csv.gz").to_string(),
            (files_list_prefix.clone() + "/*.csv").to_string(),
        ];
        let vertex_files = get_files_list( &self.input_dir, &files_list_input).unwrap();
        let parser = LDBCVertexParser::new(self.graph_schema.get_vertex_label_id(label_name).unwrap(), 1);
        for vertex_file in vertex_files.iter() {
            if vertex_file.clone().to_str().unwrap().ends_with(".csv.gz") {
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
                        results.push(vertex_meta.global_id);
                    }
                }
            }
        }

        Ok(results)
    }

    fn load_edges_to_delete<I>(&self, graph: &mut GraphDB<G, I>, edge_name: &str, batch: &str, src_label: LabelId, edge_label: LabelId, dst_label: LabelId) -> GDBResult<()> where I: Send + Sync + IndexType {
        let index = graph.modification.edge_label_to_index(src_label, dst_label, edge_label);
        let files_list_prefix = "/deletes/dynamic/".to_string() + edge_name + "/batch_id=" + batch;
        let files_list_input = vec![
            (files_list_prefix.clone() + "/*.csv.gz").to_string(),
            (files_list_prefix.clone() + "/*.csv").to_string(),
        ];
        let edge_files = get_files_list(&self.input_dir, &files_list_input).unwrap();
        let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
        parser.with_endpoint_col_id(1, 2);
        for edge_file in edge_files.iter() {
            if edge_file.clone().to_str().unwrap().ends_with(".csv.gz") {
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
                        graph.delete_edge_opt(index, edge_meta.src_global_id, edge_meta.dst_global_id);
                    }
                }
            }
        }

        Ok(())
    }

    fn delete_comments<I>(&mut self, graph: &mut GraphDB<G, I>) -> GDBResult<()> where I: Send + Sync + IndexType {
        let comment_label = self.graph_schema.get_vertex_label_id("COMMENT").unwrap();
        for comment in self.comments_to_delete.iter() {
            graph.delete_vertex(comment_label, *comment);
        }
        self.comments_to_delete.clear();

        Ok(())
    }

    fn delete_posts<I>(&mut self, graph: &mut GraphDB<G, I>) -> GDBResult<()> where I: Send + Sync + IndexType {
        let post_label = self.graph_schema.get_vertex_label_id("POST").unwrap();
        for post in self.posts_to_delete.iter() {
            graph.delete_vertex(post_label, *post);
        }
        self.posts_to_delete.clear();

        let replyOf_label = self.graph_schema.get_edge_label_id("REPLYOF").unwrap();
        let comment_label = self.graph_schema.get_vertex_label_id("COMMENT").unwrap();
        let post_replyOf_comment = graph.get_sub_graph(post_label, replyOf_label, comment_label, Direction::Outgoing);
        for post in self.posts_to_delete.iter() {
            let (got_label, lid) = graph.vertex_map.get_internal_id(*post).unwrap();
            if got_label != post_label {
                continue;
            }
            for e in post_replyOf_comment.get_adj_list(lid).unwrap() {
                let oid = graph.vertex_map.get_global_id(comment_label, *e).unwrap();
                self.comments_to_delete.push(oid);
            }
        }
        Ok(())
    }

    fn delete_forums<I>(&mut self, graph: &mut GraphDB<G, I>) -> GDBResult<()> where I: Send + Sync + IndexType {
        let forum_label = self.graph_schema.get_vertex_label_id("FORUM").unwrap();
        for forum in self.forums_to_delete.iter() {
            graph.delete_vertex(forum_label, *forum);
        }
        self.forums_to_delete.clear();

        let containerOf_label = self.graph_schema.get_edge_label_id("CONTAINEROF").unwrap();
        let post_label = self.graph_schema.get_vertex_label_id("POST").unwrap();
        let forum_containerOf_post = graph.get_sub_graph(forum_label, containerOf_label, post_label, Direction::Outgoing);
        for forum in self.forums_to_delete.iter() {
            let (got_label, lid) = graph.vertex_map.get_internal_id(*forum).unwrap();
            if got_label != forum_label {
                continue;
            }
            for e in forum_containerOf_post.get_adj_list(lid).unwrap() {
                let oid = graph.vertex_map.get_global_id(post_label, *e).unwrap();
                self.posts_to_delete.push(oid);
            }
        }

        Ok(())
    }

    fn delete_persons<I>(&mut self, graph: &mut GraphDB<G, I>) -> GDBResult<()> where I: Send + Sync + IndexType {
        let person_label = self.graph_schema.get_vertex_label_id("PERSON").unwrap();
        for person in self.persons_to_delete.iter() {
            graph.delete_vertex(person_label, *person);
        }
        self.persons_to_delete.clear();

        let hasCreator_label = self.graph_schema.get_edge_label_id("HASCREATOR").unwrap();
        let comment_label = self.graph_schema.get_vertex_label_id("COMMENT").unwrap();
        let post_label = self.graph_schema.get_vertex_label_id("POST").unwrap();
        let hasModerator_label = self.graph_schema.get_edge_label_id("HASMODERATOR").unwrap();
        let forum_label = self.graph_schema.get_vertex_label_id("FORUM").unwrap();

        let comment_hasCreator_person = graph.get_sub_graph(comment_label, hasCreator_label, person_label, Direction::Incoming);
        let post_hasCreator_person = graph.get_sub_graph(post_label, hasCreator_label, person_label, Direction::Incoming);
        let forum_hasModerator_person = graph.get_sub_graph(forum_label, hasModerator_label, person_label, Direction::Incoming);

        let forum_title_column = graph.vertex_prop_table[forum_label as usize].get_column_by_name("title").as_any().downcast_ref::<StringColumn>().unwrap();
        for person in self.persons_to_delete.iter() {
            let (got_label, lid) = graph.vertex_map.get_internal_id(*person).unwrap();
            if got_label != person_label {
                continue;
            }
            for e in comment_hasCreator_person.get_adj_list(lid).unwrap() {
                let oid = graph.vertex_map.get_global_id(comment_label, *e).unwrap();
                self.comments_to_delete.push(oid);
            }
            for e in post_hasCreator_person.get_adj_list(lid).unwrap() {
                let oid = graph.vertex_map.get_global_id(post_label, *e).unwrap();
                self.posts_to_delete.push(oid);
            }
            for e in forum_hasModerator_person.get_adj_list(lid).unwrap() {
                let title = forum_title_column.get(e.index() as usize).unwrap();
                let title_string = title.to_string();
                if title_string.starts_with("Album") || title_string.starts_with("Wall") {
                    let oid = graph.vertex_map.get_global_id(forum_label, *e).unwrap();
                    self.forums_to_delete.push(oid);
                }
            }
        }

        Ok(())
    }

    fn apply_deletes<I>(&mut self, graph: &mut GraphDB<G, I>, batch: &str) -> GDBResult<()> where I: Send + Sync + IndexType {
        self.persons_to_delete = self.load_vertices_to_delete("Person", batch)?;
        self.forums_to_delete = self.load_vertices_to_delete("Forum", batch)?;
        self.posts_to_delete = self.load_vertices_to_delete("Post", batch)?;
        self.comments_to_delete = self.load_vertices_to_delete("Comment", batch)?;

        self.delete_persons(graph)?;
        self.delete_forums(graph)?;
        self.delete_posts(graph)?;
        self.delete_comments(graph)?;

        let edges_to_delete = vec![
            (("COMMENT", "HASCREATOR", "PERSON"), "Comment_hasCreator_Person"),
            (("COMMENT", "ISLOCATEDIN", "COUNTRY"), "Comment_isLocatedIn_Country"),
            (("COMMENT", "REPLYOF", "COMMENT"), "Comment_replyOf_Comment"),
            (("COMMENT", "REPLYOF", "POST"), "Comment_replyOf_Post"),
            (("FORUM", "CONTAINEROF", "POST"), "Forum_containerOf_Post"),
            (("FORUM", "HASMEMBER", "PERSON"), "Forum_hasMember_Person"),
            (("FORUM", "HASMODERATOR", "PERSON"), "Forum_hasModerator_Person"),
            (("PERSON", "ISLOCATEDIN", "CITY"), "Person_isLocatedIn_City"),
            (("PERSON", "KNOWS", "PERSON"), "Person_knows_Person"),
            (("PERSON", "LIKES", "COMMENT"), "Person_likes_Comment"),
            (("PERSON", "LIKES", "POST"), "Person_likes_Post"),
            (("POST", "HASCREATOR", "PERSON"), "Post_hasCreator_Person"),
            (("POST", "ISLOCATEDIN", "COUNTRY"), "Post_isLocatedIn_Country"),
        ];

        for (edge, edge_name) in edges_to_delete {
            let src_label = self.graph_schema.get_vertex_label_id(edge.0).unwrap();
            let edge_label = self.graph_schema.get_edge_label_id(edge.1).unwrap();
            let dst_label = self.graph_schema.get_vertex_label_id(edge.2).unwrap();

            self.load_edges_to_delete(graph, edge_name, batch, src_label, edge_label, dst_label)?;
        }

        Ok(())
    }

    fn apply_vertices_inserts<I>(&mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema) -> GDBResult<()> where I: Send + Sync + IndexType {
        let v_label_num = graph.vertex_label_num;
        for v_label_i in 0..v_label_num {
            if let Some(vertex_file_strings) = input_schema.get_vertex_file(v_label_i as LabelId) {
                if vertex_file_strings.is_empty() {
                    continue;
                }

                let input_header = input_schema.get_vertex_header(v_label_i as LabelId).unwrap();
                let graph_header = self.graph_schema.get_vertex_header(v_label_i as LabelId).unwrap();
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
                let vertex_files_prefix = self.input_dir.clone().join("/inserts/");

                let vertex_files = get_files_list(&vertex_files_prefix, &vertex_file_strings).unwrap();
                for vertex_file in vertex_files.iter() {
                    if vertex_file.clone().to_str().unwrap().ends_with(".csv.gz") {
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
                                if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                                    graph.insert_vertex(vertex_meta.label, vertex_meta.global_id, Some(properties));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn apply_edges_inserts<I>(&mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema) -> GDBResult<()> where I: Send + Sync + IndexType {
        let vertex_label_num = graph.vertex_label_num;
        let edge_label_num = graph.edge_label_num;

        for e_label_i in 0..edge_label_num {
            for src_label_i in 0..vertex_label_num {
                for dst_label_i in 0..vertex_label_num {
                    if let Some(edge_file_strings) = input_schema.get_edge_file(src_label_i as LabelId, e_label_i as LabelId, dst_label_i as LabelId) {
                        if edge_file_strings.is_empty() {
                            continue;
                        }

                        let input_header = input_schema.get_edge_header(src_label_i as LabelId, e_label_i as LabelId, dst_label_i as LabelId).unwrap();
                        let graph_header = self.graph_schema.get_edge_header(src_label_i as LabelId, e_label_i as LabelId, dst_label_i as LabelId).unwrap();
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

                        let mut parser = LDBCEdgeParser::<G>::new(src_label_i as LabelId, dst_label_i as LabelId, e_label_i as LabelId);
                        let index = graph.modification.edge_label_to_index(src_label_i as LabelId, dst_label_i as LabelId, e_label_i as LabelId);

                        let edge_files_prefix = self.input_dir.clone().join("/inserts/");
                        let edge_files = get_files_list(&edge_files_prefix, &edge_file_strings).unwrap();

                        for edge_file in edge_files.iter() {
                            if edge_file.clone().to_str().unwrap().ends_with(".csv.gz") {
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
                                        if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                                            if (properties.is_empty()) {
                                                graph.insert_edge_opt(index, edge_meta.src_global_id, edge_meta.dst_global_id, None);
                                            } else {
                                                graph.insert_edge_opt(index, edge_meta.src_global_id, edge_meta.dst_global_id, Some(properties));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn apply_inserts<I>(&mut self, graph: &mut GraphDB<G, I>, input_schema_file: &PathBuf) -> GDBResult<()> where I: Send + Sync + IndexType {
        let input_schema = InputSchema::from_json_file(input_schema_file, &self.graph_schema).unwrap();
        self.apply_vertices_inserts(graph, &input_schema)?;
        self.apply_edges_inserts(graph, &input_schema)?;
        Ok(())
    }

    pub fn modify<I>(&mut self, graph: &mut GraphDB<G, I>, batch: &str, insert_schema_file: &PathBuf) -> GDBResult<()> where I: Send + Sync + IndexType {
        self.apply_deletes(graph, batch)?;
        self.apply_inserts(graph, insert_schema_file)?;
        graph.apply_modifications();
        Ok(())
    }
}
