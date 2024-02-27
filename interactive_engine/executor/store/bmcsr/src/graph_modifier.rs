use crate::col_table::{ColTable, parse_properties};
use crate::columns::{Column, StringColumn};
use csv::ReaderBuilder;
use rust_htslib::bgzf::Reader as GzReader;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::format;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

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
        input_dir: D, graph_schema_file: D, work_id: usize, peers: usize,
    ) -> GraphModifier<G> {
        let graph_schema =
            CsrGraphSchema::from_json_file(graph_schema_file).expect("Read trim schema error!");
        // graph_schema.desc();

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

    fn load_vertices_to_delete(&self, label_name: &str, batch: &str) -> GDBResult<Vec<G>> {
        let mut results = vec![];
        let files_list_prefix = "/deletes/dynamic/".to_string() + label_name + "/batch_id=" + batch;
        let files_list_input = vec![
            (files_list_prefix.clone() + "/*.csv.gz").to_string(),
            (files_list_prefix.clone() + "/*.csv").to_string(),
        ];
        let vertex_files = get_files_list(&self.input_dir, &files_list_input).unwrap();
        let parser = LDBCVertexParser::new(
            self.graph_schema
                .get_vertex_label_id(label_name.to_uppercase().as_str())
                .unwrap(),
            1,
        );
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
                        results.push(vertex_meta.global_id);
                    }
                }
            }
        }

        Ok(results)
    }

    fn load_edges_to_delete<I>(
        &self, graph: &mut GraphDB<G, I>, edge_name: &str, batch: &str, src_label: LabelId,
        edge_label: LabelId, dst_label: LabelId,
    ) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
    {
        let index = graph
            .modification
            .edge_label_to_index(src_label, dst_label, edge_label);
        let files_list_prefix = "/deletes/dynamic/".to_string() + edge_name + "/batch_id=" + batch;
        let files_list_input = vec![
            (files_list_prefix.clone() + "/*.csv.gz").to_string(),
            (files_list_prefix.clone() + "/*.csv").to_string(),
        ];
        let edge_files = get_files_list(&self.input_dir, &files_list_input).unwrap();
        let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
        parser.with_endpoint_col_id(1, 2);
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
                        graph.delete_edge_opt(index, edge_meta.src_global_id, edge_meta.dst_global_id);
                    }
                }
            }
        }

        Ok(())
    }

    fn delete_comments<I>(&mut self, graph: &mut GraphDB<G, I>) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
    {
        let comment_label = self
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();
        let person_label = self
            .graph_schema
            .get_vertex_label_id("PERSON")
            .unwrap();
        let place_label = self
            .graph_schema
            .get_vertex_label_id("PLACE")
            .unwrap();
        let tag_label = self
            .graph_schema
            .get_vertex_label_id("TAG")
            .unwrap();

        let replyOf_label = self
            .graph_schema
            .get_edge_label_id("REPLYOF")
            .unwrap();
        let likes_label = self
            .graph_schema
            .get_edge_label_id("LIKES")
            .unwrap();
        let hasCreator_label = self
            .graph_schema
            .get_edge_label_id("HASCREATOR")
            .unwrap();
        let isLocatedIn_label = self
            .graph_schema
            .get_edge_label_id("ISLOCATEDIN")
            .unwrap();
        let hasTag_label = self
            .graph_schema
            .get_edge_label_id("HASTAG")
            .unwrap();

        let comment_replyOf_comment =
            graph.get_sub_graph(comment_label, replyOf_label, comment_label, Direction::Incoming);

        let person_likes_comment = graph.get_sub_graph(comment_label, likes_label, person_label, Direction::Incoming);
        let person_likes_comment_out_index = graph.edge_label_to_index(person_label, comment_label, likes_label, Direction::Outgoing);

        let comment_hasCreator_person =
            graph.get_single_sub_graph(comment_label, hasCreator_label, person_label, Direction::Outgoing);
        let comment_hasCreator_person_in_index = graph.edge_label_to_index(person_label, comment_label, hasCreator_label, Direction::Incoming);

        let comment_isLocatedIn_place =
            graph.get_single_sub_graph(comment_label, isLocatedIn_label, place_label, Direction::Outgoing);
        let comment_isLocatedIn_place_in_index = graph.edge_label_to_index(place_label, comment_label, isLocatedIn_label, Direction::Incoming);

        let comment_hasTag_tag =
            graph.get_sub_graph(comment_label, hasTag_label, tag_label, Direction::Outgoing);
        let comment_hasTag_tag_in_index = graph.edge_label_to_index(tag_label, comment_label, hasTag_label, Direction::Incoming);

        let mut index = 0;
        while index < self.comments_to_delete.len() {
            let (got_label, lid) = graph.vertex_map.get_internal_id(self.comments_to_delete[index]).unwrap();
            index += 1;
            if got_label != comment_label {
                continue;
            }
            let replies = comment_replyOf_comment.get_adj_list(lid).unwrap();
            for e in replies {
                let oid = graph
                    .vertex_map
                    .get_global_id(comment_label, *e)
                    .unwrap();
                self.comments_to_delete.push(oid);
            }

            // graph.delete_vertex(comment_label, self.comments_to_delete[index - 1]);

            // for e in person_likes_comment.get_adj_list(lid).unwrap() {
            //     graph.delete_outgoing_edge_opt(person_likes_comment_out_index,self.comments_to_delete[index - 1], *e);
            // }

            // for e in comment_hasCreator_person.get_adj_list(lid).unwrap() {
            //     graph.delete_incoming_edge_opt(comment_hasCreator_person_in_index, *e, self.comments_to_delete[index - 1]);
            // }

            // for e in comment_isLocatedIn_place.get_adj_list(lid).unwrap() {
            //     graph.delete_incoming_edge_opt(comment_isLocatedIn_place_in_index, *e, self.comments_to_delete[index - 1]);
            // }

            // for e in comment_hasTag_tag.get_adj_list(lid).unwrap() {
            //     graph.delete_incoming_edge_opt(comment_hasTag_tag_in_index, *e, self.comments_to_delete[index - 1]);
            // }
        }
        self.comments_to_delete.clear();

        Ok(())
    }

    fn delete_posts<I>(&mut self, graph: &mut GraphDB<G, I>) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
    {
        let post_label = self
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        let comment_label = self
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();
        let forum_label = self
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();
        let person_label = self
            .graph_schema
            .get_vertex_label_id("PERSON")
            .unwrap();
        let place_label = self
            .graph_schema
            .get_vertex_label_id("PLACE")
            .unwrap();
        let tag_label = self
            .graph_schema
            .get_vertex_label_id("TAG")
            .unwrap();

        let replyOf_label = self
            .graph_schema
            .get_edge_label_id("REPLYOF")
            .unwrap();
        let likes_label = self
            .graph_schema
            .get_edge_label_id("LIKES")
            .unwrap();
        let hasCreator_label = self
            .graph_schema
            .get_edge_label_id("HASCREATOR")
            .unwrap();
        let isLocatedIn_label = self
            .graph_schema
            .get_edge_label_id("ISLOCATEDIN")
            .unwrap();
        let containerOf_label = self
            .graph_schema
            .get_edge_label_id("CONTAINEROF")
            .unwrap();
        let hasTag_label = self
            .graph_schema
            .get_edge_label_id("HASTAG")
            .unwrap();

        let comment_replyOf_post =
            graph.get_sub_graph(post_label, replyOf_label, comment_label, Direction::Incoming);
        let person_likes_post =
            graph.get_sub_graph(post_label, likes_label, person_label, Direction::Incoming);
        let person_likes_post_out_index = graph.edge_label_to_index(person_label, post_label, likes_label, Direction::Outgoing);
        let post_hasCreator_person =
            graph.get_single_sub_graph(post_label, hasCreator_label, person_label, Direction::Outgoing);
        let post_hasCreator_person_in_index = graph.edge_label_to_index(person_label, post_label, hasCreator_label, Direction::Incoming);
        let post_isLocatedIn_place =
            graph.get_single_sub_graph(post_label, isLocatedIn_label, place_label, Direction::Outgoing);
        let post_isLocatedIn_place_in_index = graph.edge_label_to_index(place_label, post_label, isLocatedIn_label, Direction::Incoming);
        let forum_containerOf_post =
            graph.get_single_sub_graph(forum_label, containerOf_label, post_label, Direction::Outgoing);
        let forum_containerOf_post_in_index = graph.edge_label_to_index(post_label, forum_label, containerOf_label, Direction::Incoming);
        let post_hasTag_tag =
            graph.get_sub_graph(post_label, hasTag_label, tag_label, Direction::Outgoing);
        let post_hasTag_tag_in_index = graph.edge_label_to_index(tag_label, post_label, hasTag_label, Direction::Incoming);


        /*
        for post in self.posts_to_delete.iter() {
            let (got_label: lid) = graph.vertex_map.get_internal_id(*post).unwrap();
            if got_label != post_label {
                continue;
            }

            graph.delete_vertex(post_label, *post);

            for e in comment_replyOf_post.get_adj_list(lid).unwrap() {
                let oid = graph
                    .vertex_map
                    .get_global_id(comment_label, *e)
                    .unwrap();
                self.comments_to_delete.push(oid);
            }
            for e in person_likes_post.get_adj_list(lid).unwrap() {
                graph.delete_outgoing_edge_opt(person_likes_post_out_index, *e, lid);
            }
            for e in post_hasCreator_person.get_adj_list(lid).unwrap() {
                graph.delete_incoming_edge_opt(post_hasCreator_person_in_index, lid, *e);
            }
            for e in post_isLocatedIn_place.get_adj_list(lid).unwrap() {
                graph.delete_incoming_edge_opt(post_isLocatedIn_place_in_index, lid, *e);
            }
            for e in forum_containerOf_post.get_adj_list(lid).unwrap() {
                graph.delete_incoming_edge_opt(forum_containerOf_post_in_index, *e, lid);
            }
            for e in post_hasTag_tag.get_adj_list(lid).unwrap() {
                graph.delete_incoming_edge_opt(post_hasTag_tag_in_index, lid, *e);
            }
        }
         */
        self.posts_to_delete.clear();
        Ok(())
    }

    fn delete_forums<I>(&mut self, graph: &mut GraphDB<G, I>) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
    {
        let forum_label = self
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();
        let person_label = self
            .graph_schema
            .get_vertex_label_id("PERSON")
            .unwrap();
        let post_label = self
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        let tag_label = self
            .graph_schema
            .get_vertex_label_id("TAG")
            .unwrap();

        let hasMember_label = self
            .graph_schema
            .get_edge_label_id("HASMEMBER")
            .unwrap();
        let hasModerator_label = self
            .graph_schema
            .get_edge_label_id("HASMODERATOR")
            .unwrap();
        let containerOf_label = self
            .graph_schema
            .get_edge_label_id("CONTAINEROF")
            .unwrap();
        let hasTag_label = self
            .graph_schema
            .get_edge_label_id("HASTAG")
            .unwrap();

        let forum_hasMember_person =
            graph.get_sub_graph(forum_label, hasMember_label, person_label, Direction::Outgoing);
        let forum_hasMember_person_in_index = graph.edge_label_to_index(person_label, forum_label, hasMember_label, Direction::Incoming);
        let forum_hasModerator_person =
            graph.get_single_sub_graph(forum_label, hasModerator_label, person_label, Direction::Outgoing);
        let forum_hasModerator_person_in_index = graph.edge_label_to_index(person_label, forum_label, hasModerator_label, Direction::Incoming);
        let forum_containerOf_post =
            graph.get_sub_graph(forum_label, containerOf_label, post_label, Direction::Outgoing);
        let forum_containerOf_post_in_index = graph.edge_label_to_index(post_label, forum_label, containerOf_label, Direction::Incoming);
        let forum_hasTag_tag =
            graph.get_sub_graph(forum_label, hasTag_label, tag_label, Direction::Outgoing);
        let forum_hasTag_tag_in_index = graph.edge_label_to_index(tag_label, forum_label, hasTag_label, Direction::Incoming);

        for forum in self.forums_to_delete.iter() {
            let (got_label, lid) = graph.vertex_map.get_internal_id(*forum).unwrap();
            if got_label != forum_label {
                continue;
            }

            // graph.delete_vertex(forum_label, *forum);

            // for e in forum_hasMember_person.get_adj_list(lid).unwrap() {
            //     graph.delete_incoming_edge_opt(forum_hasMember_person_in_index, lid, *e);
            // }
            // for e in forum_hasModerator_person.get_adj_list(lid).unwrap() {
            //     graph.delete_incoming_edge_opt(forum_hasModerator_person_in_index, lid, *e);
            // }
            // for e in forum_containerOf_post.get_adj_list(lid).unwrap() {
            //     graph.delete_incoming_edge_opt(forum_containerOf_post_in_index, lid, *e);
            // }
            // for e in forum_hasTag_tag.get_adj_list(lid).unwrap() {
            //     graph.delete_incoming_edge_opt(forum_hasTag_tag_in_index, lid, *e);
            // }
        }
        self.forums_to_delete.clear();
        Ok(())
    }

    fn delete_persons<I>(&mut self, graph: &mut GraphDB<G, I>) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
    {
        let person_label = self
            .graph_schema
            .get_vertex_label_id("PERSON")
            .unwrap();
        let comment_label = self
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();
        let post_label = self
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        let forum_label = self
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();
        let organization_label = self
            .graph_schema
            .get_vertex_label_id("ORGANIZATION")
            .unwrap();
        let tag_label = self
            .graph_schema
            .get_vertex_label_id("TAG")
            .unwrap();
        let place_label = self
            .graph_schema
            .get_vertex_label_id("PLACE")
            .unwrap();

        let hasCreator_label = self
            .graph_schema
            .get_edge_label_id("HASCREATOR")
            .unwrap();
        let hasModerator_label = self
            .graph_schema
            .get_edge_label_id("HASMODERATOR")
            .unwrap();


        for person in self.persons_to_delete.iter() {
            graph.delete_vertex(person_label, *person);
        }
        self.persons_to_delete.clear();


        let comment_hasCreator_person =
            graph.get_sub_graph(person_label, hasCreator_label, comment_label, Direction::Incoming);
        let post_hasCreator_person =
            graph.get_sub_graph(person_label, hasCreator_label, post_label, Direction::Incoming);
        let forum_hasModerator_person =
            graph.get_sub_graph(person_label, hasModerator_label, forum_label, Direction::Incoming);
        let forum_hasModerator_person_out_index = graph.edge_label_to_index(forum_label, person_label, hasModerator_label, Direction::Outgoing);

        let person_likes_comment = graph.get_sub_graph(comment_label, hasCreator_label, person_label, Direction::Outgoing);
        let person_likes_comment_in_index = graph.edge_label_to_index(person_label, comment_label, hasCreator_label, Direction::Incoming);
        let person_likes_post = graph.get_sub_graph(post_label, hasCreator_label, person_label, Direction::Outgoing);
        let person_likes_post_in_index = graph.edge_label_to_index(person_label, post_label, hasCreator_label, Direction::Incoming);
        let person_knows_person_out = graph.get_sub_graph(person_label, hasCreator_label, person_label, Direction::Outgoing);
        let person_knows_person_in = graph.get_sub_graph(person_label, hasCreator_label, person_label, Direction::Incoming);
        let person_knows_person_out_index = graph.edge_label_to_index(person_label, person_label, hasCreator_label, Direction::Outgoing);
        let person_knows_person_in_index = graph.edge_label_to_index(person_label, person_label, hasCreator_label, Direction::Incoming);
        let person_workAt_place = graph.get_sub_graph(person_label, hasCreator_label, person_label, Direction::Outgoing);
        let person_workAt_place_in_index = graph.edge_label_to_index(place_label, person_label, hasCreator_label, Direction::Incoming);

        let forum_title_column = graph.vertex_prop_table[forum_label as usize]
            .get_column_by_name("title")
            .as_any()
            .downcast_ref::<StringColumn>()
            .unwrap();
        for person in self.persons_to_delete.iter() {
            let (got_label, lid) = graph
                .vertex_map
                .get_internal_id(*person)
                .unwrap();
            if got_label != person_label {
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
                self.comments_to_delete.push(oid);
            }
            for e in post_hasCreator_person
                .get_adj_list(lid)
                .unwrap()
            {
                let oid = graph
                    .vertex_map
                    .get_global_id(post_label, *e)
                    .unwrap();
                self.posts_to_delete.push(oid);
            }
            for e in forum_hasModerator_person
                .get_adj_list(lid)
                .unwrap()
            {
                let title = forum_title_column
                    .get(e.index() as usize)
                    .unwrap();
                let title_string = title.to_string();
                if title_string.starts_with("Album") || title_string.starts_with("Wall") {
                    let oid = graph
                        .vertex_map
                        .get_global_id(forum_label, *e)
                        .unwrap();
                    self.forums_to_delete.push(oid);
                }
            }
        }

        Ok(())
    }


    fn apply_edges_deletes<I>(&mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema) -> GDBResult<()> where
        I: Send + Sync + IndexType,
    {
        return Ok(())
    }


    fn apply_deletes<I>(&mut self, graph: &mut GraphDB<G, I>, delete_schema_file: &PathBuf) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
    {
        let input_schema = InputSchema::from_json_file(delete_schema_file, &graph.graph_schema).unwrap();
        let vertex_label_num = graph.vertex_label_num;
        let edge_label_num = graph.edge_label_num;
        let mut delete_sets = vec![];
        for v_label_i in 0..vertex_label_num {
            let mut delete_set = HashSet::new();
            if let Some(vertex_file_strings) = input_schema.get_vertex_file(v_label_i as LabelId) {
                if !vertex_file_strings.is_empty() {
                    let vertex_files_prefix = self.input_dir.clone().join("deletes");
                    let vertex_files = get_files_list(&vertex_files_prefix, &vertex_file_strings).unwrap();
                    let parser = LDBCVertexParser::<G>::new(v_label_i as LabelId, 1);
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
                                    let (got_label, lid) = graph.vertex_map.get_internal_id(vertex_meta.global_id).unwrap();
                                    if got_label == v_label_i as LabelId {
                                        delete_set.insert(lid);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            delete_sets.push(delete_set);
        }

        for e_label_i in 0..edge_label_num {
            for src_label_i in 0..vertex_label_num {
                let src_delete_set = &delete_sets[src_label_i];
                for dst_label_i in 0..vertex_label_num {
                    let dst_delete_set = &delete_sets[dst_label_i];
                    let mut delete_edge_set = HashSet::new();
                    if let Some(edge_file_strings) = input_schema.get_edge_file(
                        src_label_i as LabelId,
                        e_label_i as LabelId,
                        dst_label_i as LabelId,
                    ) {
                        if edge_file_strings.is_empty() {
                            continue;
                        }

                        let src_col_id = 1;
                        let dst_col_id = 2;

                        let mut parser = LDBCEdgeParser::<G>::new(
                            src_label_i as LabelId,
                            dst_label_i as LabelId,
                            e_label_i as LabelId,
                        );
                        parser.with_endpoint_col_id(src_col_id, dst_col_id);
                        let index = graph.modification.edge_label_to_index(
                            src_label_i as LabelId,
                            dst_label_i as LabelId,
                            e_label_i as LabelId,
                        );

                        let edge_files_prefix = self.input_dir.clone().join("inserts");
                        let edge_files = get_files_list(&edge_files_prefix, &edge_file_strings).unwrap();

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
                                        let (got_src_label, src_lid) = graph.vertex_map.get_internal_id(edge_meta.src_global_id).unwrap();
                                        let (got_dst_label, dst_lid) = graph.vertex_map.get_internal_id(edge_meta.dst_global_id).unwrap();
                                        if got_src_label != src_label_i as LabelId || got_dst_label == dst_label_i as LabelId {
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

                    if src_delete_set.is_empty() && dst_delete_set.is_empty() && delete_edge_set.is_empty() {
                        continue;
                    }
                    graph.delete_edges(src_label_i as LabelId, e_label_i as LabelId, dst_label_i as LabelId, src_delete_set, dst_delete_set, &delete_edge_set);
                }
            }
        }

        /*
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
            (("COMMENT", "ISLOCATEDIN", "PLACE"), "Comment_isLocatedIn_Country"),
            (("COMMENT", "REPLYOF", "COMMENT"), "Comment_replyOf_Comment"),
            (("COMMENT", "REPLYOF", "POST"), "Comment_replyOf_Post"),
            (("FORUM", "CONTAINEROF", "POST"), "Forum_containerOf_Post"),
            (("FORUM", "HASMEMBER", "PERSON"), "Forum_hasMember_Person"),
            (("FORUM", "HASMODERATOR", "PERSON"), "Forum_hasModerator_Person"),
            (("PERSON", "ISLOCATEDIN", "PLACE"), "Person_isLocatedIn_City"),
            (("PERSON", "KNOWS", "PERSON"), "Person_knows_Person"),
            (("PERSON", "LIKES", "COMMENT"), "Person_likes_Comment"),
            (("PERSON", "LIKES", "POST"), "Person_likes_Post"),
            (("POST", "HASCREATOR", "PERSON"), "Post_hasCreator_Person"),
            (("POST", "ISLOCATEDIN", "PLACE"), "Post_isLocatedIn_Country"),
        ];

        for (edge, edge_name) in edges_to_delete {
            let src_label = self
                .graph_schema
                .get_vertex_label_id(edge.0)
                .unwrap();
            let edge_label = self
                .graph_schema
                .get_edge_label_id(edge.1)
                .unwrap();
            let dst_label = self
                .graph_schema
                .get_vertex_label_id(edge.2)
                .unwrap();

            self.load_edges_to_delete(graph, edge_name, batch, src_label, edge_label, dst_label)?;
        }
         */

        Ok(())
    }

    fn apply_vertices_inserts<I>(
        &mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema,
    ) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
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
                let graph_header = self
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
                let vertex_files_prefix = self.input_dir.clone().join("inserts");

                let vertex_files = get_files_list(&vertex_files_prefix, &vertex_file_strings).unwrap();
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
                    }
                }
            }
        }

        Ok(())
    }

    fn load_insert_edges_with_no_prop(&self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
                                         input_schema: &InputSchema, graph_schema: &CsrGraphSchema,
                                         files: &Vec<PathBuf>) -> GDBResult<(Vec<(G, G)>, Option<ColTable>)>
    {
        let mut edges = vec![];

        let input_header = input_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let graph_header = graph_schema.get_edge_header(src_label, edge_label, dst_label).unwrap();
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
            if file.clone().to_str().unwrap().ends_with(".csv.gz") {
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
            }
        }

        Ok((edges, None))
    }

    fn load_insert_edges_with_prop(&self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
                                      input_schema: &InputSchema, graph_schema: &CsrGraphSchema,
                                      files: &Vec<PathBuf>) -> GDBResult<(Vec<(G, G)>, Option<ColTable>)>
    {
        let mut edges = vec![];

        let input_header = input_schema.get_edge_header(src_label, edge_label, dst_label).unwrap();
        let graph_header = graph_schema.get_edge_header(src_label, edge_label, dst_label).unwrap();
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
            if file.clone().to_str().unwrap().ends_with(".csv.gz") {
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
                        let properties = parse_properties(&record, input_header, selected.as_slice()).unwrap();
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                        prop_table.push(&properties);
                    }
                }
            }
        }

        Ok((edges, Some(prop_table)))
    }

    fn apply_edges_inserts<I>(
        &mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema,
    ) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
    {
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
                        let graph_header = self
                            .graph_schema
                            .get_edge_header(
                                src_label_i as LabelId,
                                e_label_i as LabelId,
                                dst_label_i as LabelId,
                            )
                            .unwrap();
                        let edge_files_prefix = self.input_dir.clone().join("inserts");
                        let edge_files = get_files_list(&edge_files_prefix, &edge_file_strings).unwrap();
                        let (edges, table) = if graph_header.len() > 2 {
                            self.load_insert_edges_with_prop(src_label_i as LabelId, e_label_i as LabelId, dst_label_i as LabelId,
                                                             input_schema, &self.graph_schema, &edge_files).unwrap()
                        } else {
                            self.load_insert_edges_with_no_prop(src_label_i as LabelId, e_label_i as LabelId, dst_label_i as LabelId,
                                                                input_schema, &self.graph_schema, &edge_files).unwrap()
                        };

                        graph.insert_edges(src_label_i as LabelId, e_label_i as LabelId, dst_label_i as LabelId, edges, table);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn insert<I>(&mut self, graph: &mut GraphDB<G, I>, insert_schema_file: &PathBuf) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
    {
        let input_schema = InputSchema::from_json_file(insert_schema_file, &self.graph_schema).unwrap();
        self.apply_vertices_inserts(graph, &input_schema)?;
        self.apply_edges_inserts(graph, &input_schema)?;
        graph.apply_modifications();
        Ok(())
    }

    pub fn delete<I>(&mut self, graph: &mut GraphDB<G, I>, delete_schema_file: &PathBuf) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
    {
        self.apply_deletes(graph, delete_schema_file)?;
        Ok(())
    }
}
