use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

use csv::ReaderBuilder;
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};
use rayon::prelude::*;
use rust_htslib::bgzf::Reader as GzReader;

use crate::columns::*;
use crate::dataframe::*;
use crate::error::GDBResult;
use crate::graph::Direction;
use crate::graph::IndexType;
use crate::graph_db::GraphDB;
use crate::graph_loader::get_files_list;
use crate::ldbc_parser::{LDBCEdgeParser, LDBCVertexParser};
use crate::schema::{LoadStrategy, Schema};
use crate::traverse::traverse;
use crate::types::LabelId;

#[derive(Clone, Copy)]
pub enum WriteType {
    Insert,
    Delete,
    Set,
}

#[derive(Clone)]
pub struct ColumnInfo {
    index: i32,
    name: String,
    data_type: DataType,
}

impl ColumnInfo {
    pub fn index(&self) -> i32 {
        self.index
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }
}

#[derive(Clone)]
pub struct ColumnMappings {
    column: ColumnInfo,
    property_name: String,
}

impl ColumnMappings {
    pub fn new(index: i32, name: String, data_type: DataType, property_name: String) -> Self {
        ColumnMappings { column: ColumnInfo { index, name, data_type }, property_name }
    }
}

impl Encode for ColumnMappings {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i32(self.column.index)?;
        self.column.name.write_to(writer)?;
        self.column.data_type.write_to(writer)?;
        self.property_name.write_to(writer)?;
        Ok(())
    }
}

impl Decode for ColumnMappings {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let index = reader.read_i32()?;
        let name = String::read_from(reader)?;
        let data_type = DataType::read_from(reader)?;
        let property_name = String::read_from(reader)?;
        Ok(ColumnMappings { column: ColumnInfo { index, name, data_type }, property_name })
    }
}

impl ColumnMappings {
    pub fn column(&self) -> &ColumnInfo {
        &self.column
    }

    pub fn property_name(&self) -> &String {
        &self.property_name
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum DataSource {
    File,
    Memory,
}

#[derive(Clone)]
pub struct FileInput {
    pub delimiter: String,
    pub header_row: bool,
    pub quoting: bool,
    pub quote_char: String,
    pub double_quote: bool,
    pub escape_char: String,
    pub block_size: String,
    pub location: String,
}

impl Encode for FileInput {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.delimiter.write_to(writer)?;
        self.header_row.write_to(writer)?;
        self.quoting.write_to(writer)?;
        self.quote_char.write_to(writer)?;
        self.double_quote.write_to(writer)?;
        self.escape_char.write_to(writer)?;
        self.block_size.write_to(writer)?;
        self.location.write_to(writer)?;
        Ok(())
    }
}

impl Decode for FileInput {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let delimiter = String::read_from(reader)?;
        let header_row = bool::read_from(reader)?;
        let quoting = bool::read_from(reader)?;
        let quote_char = String::read_from(reader)?;
        let double_quote = bool::read_from(reader)?;
        let escape_char = String::read_from(reader)?;
        let block_size = String::read_from(reader)?;
        let location = String::read_from(reader)?;
        Ok(FileInput {
            delimiter,
            header_row,
            quoting,
            quote_char,
            double_quote,
            escape_char,
            block_size,
            location,
        })
    }
}

impl FileInput {
    pub fn new(delimiter: String, header_row: bool, location: String) -> Self {
        FileInput {
            delimiter,
            header_row,
            quoting: true,
            quote_char: "'".to_string(),
            double_quote: true,
            escape_char: "".to_string(),
            block_size: "4Mb".to_string(),
            location,
        }
    }
}

#[derive(Clone)]
pub struct Input {
    data_source: DataSource,
    file_input: Option<FileInput>,
    memory_data: Option<DataFrame>,
}

impl Encode for Input {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self.data_source {
            DataSource::File => writer.write_u8(0)?,
            DataSource::Memory => writer.write_u8(1)?,
        };
        self.file_input.write_to(writer)?;
        self.memory_data.write_to(writer)?;
        Ok(())
    }
}

impl Decode for Input {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let data_source = match reader.read_u8()? {
            0 => DataSource::File,
            1 => DataSource::Memory,
            _ => panic!("Unknown DataSource type"),
        };
        let file_input = Option::<FileInput>::read_from(reader)?;
        let memory_data = Option::<DataFrame>::read_from(reader)?;
        Ok(Input { data_source, file_input, memory_data })
    }
}

impl Input {
    pub fn data_source(&self) -> DataSource {
        self.data_source
    }

    pub fn file_input(&self) -> Option<&FileInput> {
        self.file_input.as_ref()
    }

    pub fn memory_data(&self) -> Option<&DataFrame> {
        self.memory_data.as_ref()
    }

    pub fn take_memory_data(&mut self) -> Option<DataFrame> {
        self.memory_data.take()
    }

    pub fn file(file: FileInput) -> Self {
        Input { data_source: DataSource::File, file_input: Some(file), memory_data: None }
    }

    pub fn memory(memory_data: DataFrame) -> Self {
        Input { data_source: DataSource::Memory, file_input: None, memory_data: Some(memory_data) }
    }
}

#[derive(Clone)]
pub struct VertexMappings {
    label_id: LabelId,
    inputs: Vec<Input>,
    column_mappings: Vec<ColumnMappings>,
}

impl Encode for VertexMappings {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u8(self.label_id)?;
        self.inputs.write_to(writer)?;
        self.column_mappings.write_to(writer)?;
        Ok(())
    }
}

impl Decode for VertexMappings {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let label_id = reader.read_u8()?;
        let inputs = Vec::<Input>::read_from(reader)?;
        let column_mappings = Vec::<ColumnMappings>::read_from(reader)?;
        Ok(VertexMappings { label_id, inputs, column_mappings })
    }
}

impl VertexMappings {
    pub fn new(label_id: LabelId, inputs: Vec<Input>, column_mappings: Vec<ColumnMappings>) -> Self {
        VertexMappings { label_id, inputs, column_mappings }
    }

    pub fn vertex_label(&self) -> LabelId {
        self.label_id
    }

    pub fn inputs(&self) -> &Vec<Input> {
        &self.inputs
    }

    pub fn take_inputs(&mut self) -> Vec<Input> {
        std::mem::replace(&mut self.inputs, Vec::new())
    }

    pub fn column_mappings(&self) -> &Vec<ColumnMappings> {
        &self.column_mappings
    }
}

#[derive(Clone)]
pub struct EdgeMappings {
    src_label: LabelId,
    edge_label: LabelId,
    dst_label: LabelId,
    inputs: Vec<Input>,
    src_column_mappings: Vec<ColumnMappings>,
    dst_column_mappings: Vec<ColumnMappings>,
    column_mappings: Vec<ColumnMappings>,
}

impl Encode for EdgeMappings {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u8(self.src_label)?;
        writer.write_u8(self.edge_label)?;
        writer.write_u8(self.dst_label)?;
        self.inputs.write_to(writer)?;
        self.src_column_mappings.write_to(writer)?;
        self.dst_column_mappings.write_to(writer)?;
        self.column_mappings.write_to(writer)?;
        Ok(())
    }
}

impl Decode for EdgeMappings {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let src_label = reader.read_u8()?;
        let edge_label = reader.read_u8()?;
        let dst_label = reader.read_u8()?;
        let inputs = Vec::<Input>::read_from(reader)?;
        let src_column_mappings = Vec::<ColumnMappings>::read_from(reader)?;
        let dst_column_mappings = Vec::<ColumnMappings>::read_from(reader)?;
        let column_mappings = Vec::<ColumnMappings>::read_from(reader)?;
        Ok(EdgeMappings {
            src_label,
            edge_label,
            dst_label,
            inputs,
            src_column_mappings,
            dst_column_mappings,
            column_mappings,
        })
    }
}

impl EdgeMappings {
    pub fn new(
        src_label: LabelId, edge_label: LabelId, dst_label: LabelId, inputs: Vec<Input>,
        src_column_mappings: Vec<ColumnMappings>, dst_column_mappings: Vec<ColumnMappings>,
        column_mappings: Vec<ColumnMappings>,
    ) -> Self {
        EdgeMappings {
            src_label,
            edge_label,
            dst_label,
            inputs,
            src_column_mappings,
            dst_column_mappings,
            column_mappings,
        }
    }

    pub fn src_label(&self) -> LabelId {
        self.src_label
    }

    pub fn edge_label(&self) -> LabelId {
        self.edge_label
    }

    pub fn dst_label(&self) -> LabelId {
        self.dst_label
    }

    pub fn inputs(&self) -> &Vec<Input> {
        &self.inputs
    }

    pub fn take_inputs(&mut self) -> Vec<Input> {
        std::mem::replace(&mut self.inputs, Vec::new())
    }

    pub fn src_column_mappings(&self) -> &Vec<ColumnMappings> {
        &self.src_column_mappings
    }

    pub fn dst_column_mappings(&self) -> &Vec<ColumnMappings> {
        &self.dst_column_mappings
    }

    pub fn column_mappings(&self) -> &Vec<ColumnMappings> {
        &self.column_mappings
    }
}

#[derive(Clone)]
pub struct WriteOperation {
    write_type: WriteType,
    vertex_mappings: Option<VertexMappings>,
    edge_mappings: Option<EdgeMappings>,
}

impl Debug for WriteOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "This is a write operation")
    }
}

impl Encode for WriteOperation {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self.write_type {
            WriteType::Insert => writer.write_u8(0)?,
            WriteType::Delete => writer.write_u8(1)?,
            WriteType::Set => writer.write_u8(2)?,
        };
        self.vertex_mappings.write_to(writer)?;
        self.edge_mappings.write_to(writer)?;
        Ok(())
    }
}

impl Decode for WriteOperation {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let write_type = match reader.read_u8()? {
            0 => WriteType::Insert,
            1 => WriteType::Delete,
            2 => WriteType::Set,
            _ => panic!("Unknown write type"),
        };
        let vertex_mappings = Option::<VertexMappings>::read_from(reader)?;
        let edge_mappings = Option::<EdgeMappings>::read_from(reader)?;
        Ok(WriteOperation { write_type, vertex_mappings, edge_mappings })
    }
}

unsafe impl Send for WriteOperation {}

unsafe impl Sync for WriteOperation {}

impl WriteOperation {
    pub fn insert_vertices(vertex_mappings: VertexMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Insert,
            vertex_mappings: Some(vertex_mappings),
            edge_mappings: None,
        }
    }

    pub fn insert_edges(edge_mappings: EdgeMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Insert,
            vertex_mappings: None,
            edge_mappings: Some(edge_mappings),
        }
    }

    pub fn delete_vertices(vertex_mappings: VertexMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Delete,
            vertex_mappings: Some(vertex_mappings),
            edge_mappings: None,
        }
    }

    pub fn delete_edges(edge_mappings: EdgeMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Delete,
            vertex_mappings: None,
            edge_mappings: Some(edge_mappings),
        }
    }

    pub fn set_vertices(vertex_mappings: VertexMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Set,
            vertex_mappings: Some(vertex_mappings),
            edge_mappings: None,
        }
    }

    pub fn set_edges(edge_mappings: EdgeMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Set,
            vertex_mappings: None,
            edge_mappings: Some(edge_mappings),
        }
    }

    pub fn write_type(&self) -> WriteType {
        self.write_type
    }

    pub fn has_vertex_mappings(&self) -> bool {
        self.vertex_mappings.is_some()
    }

    pub fn vertex_mappings(&self) -> Option<&VertexMappings> {
        self.vertex_mappings.as_ref()
    }

    pub fn take_vertex_mappings(&mut self) -> Option<VertexMappings> {
        self.vertex_mappings.take()
    }

    pub fn has_edge_mappings(&self) -> bool {
        self.edge_mappings.is_some()
    }

    pub fn edge_mappings(&self) -> Option<&EdgeMappings> {
        self.edge_mappings.as_ref()
    }

    pub fn take_edge_mappings(&mut self) -> Option<EdgeMappings> {
        self.edge_mappings.take()
    }
}

pub struct AliasData {
    pub alias_index: i32,
    // pub column_data: Box<dyn Column>,
}

impl Debug for AliasData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Alias index: {}", self.alias_index)
        // write!(f, "Alias index: {}, data: {:?}", self.alias_index, self.column_data)
    }
}

impl Encode for AliasData {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i32(self.alias_index)?;
        Ok(())
    }
}

impl Decode for AliasData {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let alias_index = reader.read_i32()?;
        Ok(AliasData { alias_index })
    }
}

impl Clone for AliasData {
    fn clone(&self) -> Self {
        AliasData { alias_index: self.alias_index }
    }
}

unsafe impl Send for AliasData {}

unsafe impl Sync for AliasData {}

fn get_next_output_dir(prefix: &str, partition: usize) -> String {
    let batches = [
        "2012-11-29",
        "2012-11-30",
        "2012-12-01",
        "2012-12-02",
        "2012-12-03",
        "2012-12-04",
        "2012-12-05",
        "2012-12-06",
        "2012-12-07",
        "2012-12-08",
        "2012-12-09",
        "2012-12-10",
        "2012-12-11",
        "2012-12-12",
        "2012-12-13",
        "2012-12-14",
        "2012-12-15",
        "2012-12-16",
        "2012-12-17",
        "2012-12-18",
        "2012-12-19",
        "2012-12-20",
        "2012-12-21",
        "2012-12-22",
        "2012-12-23",
        "2012-12-24",
        "2012-12-25",
        "2012-12-26",
        "2012-12-27",
        "2012-12-28",
        "2012-12-29",
        "2012-12-30",
        "2012-12-31",
    ];
    let mut idx = 0;
    loop {
        let path = format!("{}/batch-{}/part-{}", prefix, batches[idx], partition);
        let ppath = Path::new(path.as_str());
        if ppath.exists() {
            idx += 1;
        } else {
            return path;
        }
    }
    return "".to_string();
}

pub fn apply_write_operations(
    graph: &mut GraphDB<usize, usize>, mut write_operations: Vec<WriteOperation>, servers: usize,
) {
    let mut merged_delete_vertices_data: HashMap<LabelId, Vec<u64>> = HashMap::new();
    // let mut traversed = false;
    let mut traversed = true;
    let mut insert_vertices_duration = 0.0_f64;
    let mut insert_edges_duration = 0.0_f64;
    let mut delete_edges_duration = 0.0_f64;
    let mut delete_vertices_duration = 0.0_f64;
    let mut set_vertices_duration = 0.0_f64;
    let mut set_edges_duration = 0.0_f64;
    for mut write_op in write_operations.drain(..) {
        match write_op.write_type() {
            WriteType::Insert => {
                if let Some(vertex_mappings) = write_op.take_vertex_mappings() {
                    let start = Instant::now();
                    let vertex_label = vertex_mappings.vertex_label();
                    let inputs = vertex_mappings.inputs();
                    let column_mappings = vertex_mappings.column_mappings();
                    for input in inputs.iter() {
                        insert_vertices(graph, vertex_label, input, column_mappings, servers);
                    }
                    insert_vertices_duration += start.elapsed().as_secs_f64();
                    // println!("insert vertices: {} s", start.elapsed().as_secs_f64());
                }
                if let Some(edge_mappings) = write_op.take_edge_mappings() {
                    let start = Instant::now();
                    let src_label = edge_mappings.src_label();
                    let edge_label = edge_mappings.edge_label();
                    let dst_label = edge_mappings.dst_label();
                    let inputs = edge_mappings.inputs();
                    let src_column_mappings = edge_mappings.src_column_mappings();
                    let dst_column_mappings = edge_mappings.dst_column_mappings();
                    let column_mappings = edge_mappings.column_mappings();
                    for input in inputs.iter() {
                        insert_edges(
                            graph,
                            src_label,
                            edge_label,
                            dst_label,
                            input,
                            src_column_mappings,
                            dst_column_mappings,
                            column_mappings,
                            servers,
                        );
                    }
                    insert_edges_duration += start.elapsed().as_secs_f64();
                    // println!("insert edges: {} s", start.elapsed().as_secs_f64());
                }
            }
            WriteType::Delete => {
                if let Some(mut vertex_mappings) = write_op.take_vertex_mappings() {
                    let start = Instant::now();
                    let vertex_label = vertex_mappings.vertex_label();
                    let inputs = vertex_mappings.take_inputs();
                    let column_mappings = vertex_mappings.column_mappings();
                    for mut input in inputs.into_iter() {
                        match input.data_source() {
                            DataSource::Memory => {
                                let mut id_col = -1;
                                for column_mapping in column_mappings {
                                    let column = column_mapping.column();
                                    let column_index = column.index();
                                    let property_name = column_mapping.property_name();
                                    if property_name == "id" {
                                        id_col = column_index;
                                        break;
                                    }
                                }
                                if input.data_source() == DataSource::Memory {
                                    let mut memory_data = input.take_memory_data().unwrap();
                                    let mut data = memory_data.take_columns();
                                    let vertex_id_column = data
                                        .get_mut(id_col as usize)
                                        .expect("Failed to get id column");
                                    let data = vertex_id_column.take_data();
                                    if let Some(uint64_column) = data.as_any().downcast_ref::<U64HColumn>()
                                    {
                                        if let Some(combined_data) =
                                            merged_delete_vertices_data.get_mut(&vertex_label)
                                        {
                                            combined_data.append(&mut uint64_column.data.clone())
                                        } else {
                                            merged_delete_vertices_data
                                                .insert(vertex_label, uint64_column.data.clone());
                                        }
                                    } else {
                                        panic!("Unknown data type");
                                    }
                                }
                                continue;
                            }
                            _ => {}
                        }
                        // delete_vertices(graph, vertex_label, &input, column_mappings, servers);
                    }
                    delete_vertices_duration += start.elapsed().as_secs_f64();
                    // println!("delete vertices: {} s", start.elapsed().as_secs_f64());
                }
                if let Some(edge_mappings) = write_op.take_edge_mappings() {
                    let start = Instant::now();
                    let src_label = edge_mappings.src_label();
                    let edge_label = edge_mappings.edge_label();
                    let dst_label = edge_mappings.dst_label();
                    let inputs = edge_mappings.inputs();
                    let src_column_mappings = edge_mappings.src_column_mappings();
                    let dst_column_mappings = edge_mappings.dst_column_mappings();
                    let column_mappings = edge_mappings.column_mappings();
                    for input in inputs.iter() {
                        delete_edges(
                            graph,
                            src_label,
                            edge_label,
                            dst_label,
                            input,
                            src_column_mappings,
                            dst_column_mappings,
                            column_mappings,
                            servers,
                        );
                    }
                    delete_edges_duration += start.elapsed().as_secs_f64();
                    // println!("delete edges: {} s", start.elapsed().as_secs_f64());
                }
            }
            WriteType::Set => {
                if !traversed {
                    let output_prefix =
                        get_next_output_dir("/mnt/nas/luoxiaojian/traverse_output_2", graph.partition);
                    if !output_prefix.is_empty() {
                        fs::create_dir_all(output_prefix.as_str()).unwrap();
                        traverse(&graph, output_prefix.as_str(), true);
                        traversed = true;
                    }
                }

                if let Some(mut vertex_mappings) = write_op.take_vertex_mappings() {
                    let start = Instant::now();
                    let vertex_label = vertex_mappings.vertex_label();
                    let mut inputs = vertex_mappings.take_inputs();
                    let column_mappings = vertex_mappings.column_mappings();
                    let mut column_builders = HashMap::<(LabelId, String), Box<dyn Column>>::new();

                    for input in inputs.drain(..) {
                        set_vertices(graph, vertex_label, input, column_mappings, &mut column_builders);
                    }
                    for ((vertex_label, prop_name), prop_col_builder) in column_builders.into_iter() {
                        graph.set_vertex_property(vertex_label, prop_name.as_str(), prop_col_builder);
                    }
                    set_vertices_duration += start.elapsed().as_secs_f64();
                }
                if let Some(mut edge_mappings) = write_op.take_edge_mappings() {
                    let start = Instant::now();
                    let src_label = edge_mappings.src_label();
                    let edge_label = edge_mappings.edge_label();
                    let dst_label = edge_mappings.dst_label();
                    let mut inputs = edge_mappings.take_inputs();
                    let src_column_mappings = edge_mappings.src_column_mappings();
                    let dst_column_mappings = edge_mappings.dst_column_mappings();
                    let column_mappings = edge_mappings.column_mappings();
                    let mut column_builders = HashMap::new();
                    for input in inputs.drain(..) {
                        set_edges(
                            graph,
                            src_label,
                            edge_label,
                            dst_label,
                            input,
                            src_column_mappings,
                            dst_column_mappings,
                            column_mappings,
                            &mut column_builders,
                        );
                    }
                    for ((src_label, edge_label, dst_label, col_name), (ie_cb, oe_cb)) in
                    column_builders.into_iter()
                    {
                        graph.set_edge_property(
                            dst_label,
                            edge_label,
                            src_label,
                            Direction::Incoming,
                            col_name.as_str(),
                            ie_cb,
                        );
                        graph.set_edge_property(
                            src_label,
                            edge_label,
                            dst_label,
                            Direction::Outgoing,
                            col_name.as_str(),
                            oe_cb,
                        );
                    }
                    set_edges_duration += start.elapsed().as_secs_f64();
                }
            }
        };
    }
    let start = Instant::now();
    for (vertex_label, vertex_ids) in merged_delete_vertices_data.into_iter() {
        let column_mappings =
            vec![ColumnMappings::new(0, "id".to_string(), DataType::ID, "id".to_string())];
        let input = Input::memory(DataFrame::new_vertices_ids(vertex_ids));
        delete_vertices(graph, vertex_label, &input, &column_mappings, servers);
    }
    delete_vertices_duration += start.elapsed().as_secs_f64();
    println!(
        "insert vertices: {}, insert edges: {}, delete vertices: {}, delete_edges: {}, set vertices: {}, set edges {}",
        insert_vertices_duration, insert_edges_duration, delete_vertices_duration, delete_edges_duration, set_vertices_duration, set_edges_duration,
    );
    graph.dump_schema();
}

fn insert_vertices<G, I>(
    graph: &mut GraphDB<G, I>, vertex_label: LabelId, input: &Input, column_mappings: &Vec<ColumnMappings>,
    servers: usize,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut column_map = HashMap::new();
    let mut max_col = 0;
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    let mut id_col = -1;
    if let Some((column_index, _)) = column_map.get("id") {
        id_col = *column_index;
    }
    let vertex_table_header = graph
        .graph_schema
        .get_vertex_header(vertex_label)
        .unwrap()
        .to_vec();
    for (name, _, _) in vertex_table_header.iter() {
        if !column_map.contains_key(name) {
            graph.remove_vertex_index_prop(name, vertex_label);
        }
    }
    match input.data_source() {
        DataSource::File => {
            if let Some(file_input) = input.file_input() {
                let file_location = &file_input.location;
                let path = Path::new(file_location);
                let input_dir = path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_str()
                    .unwrap()
                    .to_string();
                let filename = path
                    .file_name()
                    .expect("Can not find filename")
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                let filenames = vec![filename];
                let mut modifier = GraphModifier::new(input_dir);
                if file_input.header_row {
                    modifier.skip_header();
                }
                modifier.partitions(servers);
                let mut mappings = vec![-1; max_col as usize];
                if let Some(vertex_header) = graph
                    .graph_schema
                    .get_vertex_header(vertex_label)
                {
                    for (i, (property_name, _, _)) in vertex_header.iter().enumerate() {
                        if let Some((column_index, _)) = column_map.get(property_name) {
                            mappings[*column_index as usize] = i as i32;
                        }
                    }
                } else {
                    panic!("vertex label {} not found", vertex_label)
                }
                modifier
                    .apply_vertices_insert_with_filename(graph, vertex_label, &filenames, id_col, &mappings)
                    .unwrap();
            }
        }
        DataSource::Memory => {
            panic!("not supposed to reach here...");
        }
    }
}

pub fn insert_edges(
    graph: &mut GraphDB<usize, usize>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, input: &Input,
    src_vertex_mappings: &Vec<ColumnMappings>, dst_vertex_mappings: &Vec<ColumnMappings>,
    column_mappings: &Vec<ColumnMappings>, servers: usize,
)
{
    let mut column_map = HashMap::new();
    let mut max_col = 0;
    for column_mapping in src_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("src_id".to_string(), (column_index, data_type));
        }
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    for column_mapping in dst_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("dst_id".to_string(), (column_index, data_type));
        }
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    let edge_table_header = graph
        .graph_schema
        .get_edge_header(src_label, edge_label, dst_label)
        .unwrap()
        .to_vec();
    for (name, _, _) in edge_table_header.iter() {
        if !column_map.contains_key(name) {
            graph.remove_edge_index_prop(name, src_label, edge_label, dst_label);
        }
    }
    let mut src_id_col = -1;
    let mut dst_id_col = -1;
    if let Some((column_index, _)) = column_map.get("src_id") {
        src_id_col = *column_index;
    }
    if let Some((column_index, _)) = column_map.get("dst_id") {
        dst_id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            if let Some(file_input) = input.file_input() {
                let file_location = &file_input.location;
                let path = Path::new(file_location);
                let input_dir = path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_str()
                    .unwrap()
                    .to_string();
                let filename = path
                    .file_name()
                    .expect("Can not find filename")
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                let filenames = vec![filename];
                let mut modifier = GraphModifier::new(input_dir);
                if file_input.header_row {
                    modifier.skip_header();
                }
                modifier.partitions(servers);
                let mut mappings = vec![-1; max_col as usize];
                if let Some(edge_header) = graph
                    .graph_schema
                    .get_edge_header(src_label, edge_label, dst_label)
                {
                    for (i, (property_name, _, _)) in edge_header.iter().enumerate() {
                        if let Some((column_index, _)) = column_map.get(property_name) {
                            mappings[*column_index as usize] = i as i32;
                        }
                    }
                } else {
                    panic!("edge label {}_{}_{} not found", src_label, edge_label, dst_label)
                }
                modifier
                    .apply_edges_insert_with_filename(
                        graph, src_label, edge_label, dst_label, &filenames, src_id_col, dst_id_col,
                        &mappings,
                    )
                    .unwrap();
            }
        }
        DataSource::Memory => {
            if let Some(memory_data) = input.memory_data() {
                let data = memory_data.columns();
                let src_id_column = data
                    .get(src_id_col as usize)
                    .expect("Failed to get id column");
                let dst_id_column = data
                    .get(dst_id_col as usize)
                    .expect("Failed to get id column");
                if let Some(src_uint64_column) = src_id_column
                    .data()
                    .as_any()
                    .downcast_ref::<U64HColumn>()
                {
                    if let Some(dst_uint64_column) = dst_id_column
                        .data()
                        .as_any()
                        .downcast_ref::<U64HColumn>()
                    {
                        let edges_size = src_uint64_column.len();
                        let mut edges = Vec::with_capacity(edges_size);
                        for i in 0..edges_size {
                            edges.push((src_uint64_column.data[i] as usize, dst_uint64_column.data[i] as usize));
                        }
                        if !edges.is_empty() {
                            insert_edges_by_ids(graph, src_label, edge_label, dst_label, &edges, servers);
                        }
                    }
                }
            }
        }
    }
}

pub fn insert_edges_by_ids<G, I>(graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, edges: &Vec<(G, G)>, servers: usize)
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
{
    let start = Instant::now();
    let graph_header = graph
        .graph_schema
        .get_edge_header(src_label, edge_label, dst_label)
        .unwrap();
    let is_src_static = graph.graph_schema.is_static_vertex(src_label);
    let is_dst_static = graph.graph_schema.is_static_vertex(dst_label);
    let load_strategy = graph.graph_schema.get_edge_load_strategy(src_label, edge_label, dst_label);

    let mut corner_src_vertices = HashSet::new();
    let mut corner_dst_vertices = HashSet::new();
    if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
        for (src, dst) in edges.iter() {
            if dst.index() % servers != graph.partition {
                corner_dst_vertices.insert(*dst);
            }
        }
    }
    if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
        for (src, dst) in edges.iter() {
            if src.index() % servers != graph.partition {
                corner_src_vertices.insert(*src);
            }
        }
    }
    graph.vertex_map.insert_corner_vertices(
        src_label,
        corner_src_vertices
            .into_iter()
            .collect::<Vec<G>>(),
    );
    graph.vertex_map.insert_corner_vertices(
        dst_label,
        corner_dst_vertices
            .into_iter()
            .collect::<Vec<G>>(),
    );
    let t0 = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let parsed_edges: Vec<(I, I)> = edges
        .into_par_iter()
        .map(|(src, dst)| {
            (
                graph.vertex_map.get_internal_id(*src).unwrap().1,
                graph.vertex_map.get_internal_id(*dst).unwrap().1,
            )
        })
        .collect();
    let t1 = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let index = graph.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);

    if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyOut {
        let new_src_num = graph.vertex_map.vertex_num(src_label);
        if let Some(csr) = graph.oe.get_mut(&index) {
            csr.insert_edges_beta(
                new_src_num,
                &parsed_edges,
                None,
                false,
                graph.oe_edge_prop_table.get_mut(&index),
            );
        }
    }
    let t2 = start.elapsed().as_secs_f64();
    let start = Instant::now();

    if load_strategy == LoadStrategy::BothOutIn || load_strategy == LoadStrategy::OnlyIn {
        let new_dst_num = graph.vertex_map.vertex_num(dst_label);
        if let Some(csr) = graph.ie.get_mut(&index) {
            csr.insert_edges_beta(
                new_dst_num,
                &parsed_edges,
                None,
                true,
                graph.ie_edge_prop_table.get_mut(&index),
            );
        }
    }
    let t3 = start.elapsed().as_secs_f64();
    println!("insert edges by id: {}, {}, {}, {}", t0, t1, t2, t3);
}

pub fn delete_vertices(
    graph: &mut GraphDB<usize, usize>, vertex_label: LabelId, input: &Input,
    column_mappings: &Vec<ColumnMappings>, servers: usize,
) {
    let mut column_map = HashMap::new();
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    let mut id_col = -1;
    if let Some((column_index, _)) = column_map.get("id") {
        id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            panic!("not expect to reach here...");
        }
        DataSource::Memory => {
            if let Some(memory_data) = input.memory_data() {
                let data = memory_data.columns();
                let vertex_id_column = data
                    .get(id_col as usize)
                    .expect("Failed to get id column");
                if let Some(uint64_column) = vertex_id_column
                    .data()
                    .as_any()
                    .downcast_ref::<U64HColumn>()
                {
                    let data: Vec<usize> = uint64_column
                        .data
                        .iter()
                        .map(|&x| x as usize)
                        .collect();
                    if !data.is_empty() {
                        delete_vertices_by_ids(graph, vertex_label, &data);
                    }
                }
            }
        }
    }
}

pub fn delete_edges(
    graph: &mut GraphDB<usize, usize>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    input: &Input, src_vertex_mappings: &Vec<ColumnMappings>, dst_vertex_mappings: &Vec<ColumnMappings>,
    column_mappings: &Vec<ColumnMappings>, servers: usize,
) {
    let mut column_map = HashMap::new();
    for column_mapping in src_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("src_id".to_string(), (column_index, data_type));
        }
    }
    for column_mapping in dst_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("dst_id".to_string(), (column_index, data_type));
        }
    }
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    let mut src_id_col = -1;
    let mut dst_id_col = -1;
    if let Some((column_index, _)) = column_map.get("src_id") {
        src_id_col = *column_index;
    }
    if let Some((column_index, _)) = column_map.get("dst_id") {
        dst_id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            if let Some(file_input) = input.file_input() {
                let file_location = &file_input.location;
                let path = Path::new(file_location);
                let input_dir = path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_str()
                    .unwrap()
                    .to_string();
                let filename = path
                    .file_name()
                    .expect("Can not find filename")
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                let filenames = vec![filename];
                let mut modifier = GraphModifier::new(input_dir);
                if file_input.header_row {
                    modifier.skip_header();
                }
                modifier.partitions(servers);
                modifier
                    .apply_edges_delete_with_filename(
                        graph, src_label, edge_label, dst_label, &filenames, src_id_col, dst_id_col,
                    )
                    .unwrap();
            }
        }
        DataSource::Memory => {
            panic!("not supposed to reach here...");
        }
    }
}

pub fn delete_vertices_by_ids<G, I>(graph: &mut GraphDB<G, I>, vertex_label: LabelId, global_ids: &Vec<G>)
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
{
    let start = Instant::now();
    let mut lids = HashSet::new();
    let mut neighbors = HashSet::new();
    for v in global_ids.iter() {
        if v.index() as u64 == u64::MAX {
            continue;
        }
        if let Some(internal_id) = graph.vertex_map.get_internal_id(*v) {
            lids.insert(internal_id.1);
        }
        neighbors.insert(*v);
    }
    let t0 = start.elapsed().as_secs_f64();
    let mut t1 = 0_f64;
    let mut t2 = 0_f64;
    let mut t3 = 0_f64;

    let vertex_label_num = graph.vertex_label_num;
    let edge_label_num = graph.edge_label_num;
    for e_label_i in 0..edge_label_num {
        for src_label_i in 0..vertex_label_num {
            if graph
                .graph_schema
                .get_edge_header(src_label_i as LabelId, e_label_i as LabelId, vertex_label as LabelId)
                .is_none()
            {
                continue;
            }
            let index = graph.edge_label_to_index(
                src_label_i as LabelId,
                vertex_label as LabelId,
                e_label_i as LabelId,
                Direction::Outgoing,
            );
            if let Some(ie_csr) = graph.ie.get_mut(&index) {
                let start = Instant::now();
                ie_csr.delete_vertices(&lids);
                t1 += start.elapsed().as_secs_f64();
            }
        }
        for dst_label_i in 0..vertex_label_num {
            if graph
                .graph_schema
                .get_edge_header(vertex_label as LabelId, e_label_i as LabelId, dst_label_i as LabelId)
                .is_none()
            {
                continue;
            }
            let index = graph.edge_label_to_index(
                vertex_label as LabelId,
                dst_label_i as LabelId,
                e_label_i as LabelId,
                Direction::Outgoing,
            );
            if let Some(oe_csr) = graph.oe.get_mut(&index) {
                let start = Instant::now();
                oe_csr.delete_vertices(&lids);
                t1 += start.elapsed().as_secs_f64();
            }
        }
    }

    let start = Instant::now();
    graph
        .vertex_map
        .remove_vertices(vertex_label, &lids);

    if let Some(vertex_set) = graph.pending_to_delete.get_mut(&vertex_label) {
        vertex_set.extend(neighbors.iter());
    } else {
        graph.pending_to_delete.insert(vertex_label, neighbors);
    }

    let t4 = start.elapsed().as_secs_f64();
    println!(
        "delete_vertices_by_ids[{}][{}]: {}, {}, {}, {}, {}",
        vertex_label as usize,
        global_ids.len(),
        t0,
        t1,
        t2,
        t3,
        t4
    );
}

pub fn set_vertices(
    graph: &mut GraphDB<usize, usize>, vertex_label: LabelId, mut input: Input,
    column_mappings: &Vec<ColumnMappings>,
    column_builders: &mut HashMap<(LabelId, String), Box<dyn Column>>,
) {
    let mut column_map = HashMap::new();
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    let mut id_col = -1;
    if let Some((column_index, _)) = column_map.get("id") {
        id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            todo!()
        }
        DataSource::Memory => {
            if let Some(mut memory_data) = input.take_memory_data() {
                let mut column_data = memory_data.take_columns();
                let id_column = column_data
                    .get_mut(id_col as usize)
                    .expect("Failed to find id column");
                let data = id_column.take_data();
                let global_ids = {
                    if let Some(id_column) = data.as_any().downcast_ref::<IDHColumn>() {
                        id_column.data.clone()
                    } else if let Some(uint64_column) = data.as_any().downcast_ref::<U64HColumn>() {
                        uint64_column
                            .data
                            .par_iter()
                            .map(|&x| graph.get_internal_id(x as usize))
                            .collect()
                    } else {
                        panic!("DataType of id col is not VertexId")
                    }
                };
                for (k, v) in column_map.iter() {
                    if k == "id" {
                        continue;
                    }
                    let column_index = v.0;
                    let column_data_type = v.1;
                    let column = column_data
                        .get_mut(column_index as usize)
                        .expect("Failed to find column");
                    if let Some(cb) = column_builders.get_mut(&(vertex_label, k.clone())) {
                        cb.set_column_batch(&global_ids, &column.take_data());
                    } else {
                        let idx = graph
                            .graph_schema
                            .add_vertex_index_prop(k.clone(), vertex_label, column_data_type, false)
                            .unwrap();
                        graph.schema_updated = true;
                        let vp_prefix = format!(
                            "{}_vp_{}_col_{}",
                            graph.get_partition_prefix(),
                            vertex_label as usize,
                            idx
                        );

                        let mut cb = create_column(
                            column_data_type,
                            vp_prefix.as_str(),
                            graph.vertex_map.indexers[vertex_label as usize].len(),
                        );
                        cb.set_column_batch(&global_ids, &column.take_data());

                        column_builders.insert((vertex_label, k.clone()), cb);
                    }
                }
            }
        }
    }
}

pub fn set_edges(
    graph: &mut GraphDB<usize, usize>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    mut input: Input, src_vertex_mappings: &Vec<ColumnMappings>, dst_vertex_mappings: &Vec<ColumnMappings>,
    column_mappings: &Vec<ColumnMappings>,
    column_builders: &mut HashMap<(LabelId, LabelId, LabelId, String), (Box<dyn Column>, Box<dyn Column>)>,
) {
    let mut column_map = HashMap::new();
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    match input.data_source() {
        DataSource::File => {
            todo!()
        }
        DataSource::Memory => {
            if let Some(mut memory_data) = input.take_memory_data() {
                let mut column_data = memory_data.take_columns();
                if !src_vertex_mappings.is_empty() {
                    let offset_col_id = src_vertex_mappings[0].column().index();
                    let offset_column = column_data
                        .get_mut(offset_col_id as usize)
                        .expect("Failed to find id column");
                    let data = offset_column.take_data();
                    let offsets = {
                        if let Some(id_column) = data.as_any().downcast_ref::<IDHColumn>() {
                            id_column.data.clone()
                        } else {
                            panic!("DataType of id col is not VertexId")
                        }
                    };
                    for (k, v) in column_map.iter() {
                        let column_index = v.0;
                        let column_data_type = v.1;
                        let column = column_data
                            .get_mut(column_index as usize)
                            .expect("Failed to find column");
                        if let Some((_, oe_cb)) =
                            column_builders.get_mut(&(src_label, edge_label, dst_label, k.clone()))
                        {
                            oe_cb.set_column_batch(&offsets, &column.take_data());
                        } else {
                            let idx = graph
                                .graph_schema
                                .add_edge_index_prop(
                                    k.clone(),
                                    src_label,
                                    edge_label,
                                    dst_label,
                                    column_data_type,
                                    false
                                )
                                .unwrap();
                            graph.schema_updated = true;

                            let oe_col_size = graph.get_max_edge_offset(
                                src_label,
                                edge_label,
                                dst_label,
                                Direction::Outgoing,
                            );
                            let oe_prefix = format!(
                                "{}_oep_{}_{}_{}_col_{}",
                                graph.get_partition_prefix(),
                                src_label as usize,
                                edge_label as usize,
                                dst_label as usize,
                                idx
                            );
                            let mut oe_cb =
                                create_column(column_data_type, oe_prefix.as_str(), oe_col_size);

                            let ie_col_size = graph.get_max_edge_offset(
                                dst_label,
                                edge_label,
                                src_label,
                                Direction::Incoming,
                            );
                            let ie_prefix = format!(
                                "{}_iep_{}_{}_{}_col_{}",
                                graph.get_partition_prefix(),
                                src_label as usize,
                                edge_label as usize,
                                dst_label as usize,
                                idx
                            );
                            let ie_cb = create_column(column_data_type, ie_prefix.as_str(), ie_col_size);

                            oe_cb.set_column_batch(&offsets, &column.take_data());
                            column_builders
                                .insert((src_label, edge_label, dst_label, k.clone()), (ie_cb, oe_cb));
                        }
                    }
                }
                if !dst_vertex_mappings.is_empty() {
                    let offset_col_id = dst_vertex_mappings[0].column().index();
                    let offset_column = column_data
                        .get_mut(offset_col_id as usize)
                        .expect("Failed to find id column");
                    let data = offset_column.take_data();
                    let offsets = {
                        if let Some(id_column) = data.as_any().downcast_ref::<IDHColumn>() {
                            id_column.data.clone()
                        } else {
                            panic!("DataType of id col is not VertexId")
                        }
                    };
                    for (k, v) in column_map.iter() {
                        let column_index = v.0;
                        let column_data_type = v.1;
                        let column = column_data
                            .get_mut(column_index as usize)
                            .expect("Failed to find column");
                        if let Some((ie_cb, _)) =
                            column_builders.get_mut(&(src_label, edge_label, dst_label, k.clone()))
                        {
                            ie_cb.set_column_batch(&offsets, &column.take_data());
                        } else {
                            let idx = graph
                                .graph_schema
                                .add_edge_index_prop(
                                    k.clone(),
                                    src_label,
                                    edge_label,
                                    dst_label,
                                    column_data_type,
                                    false,
                                )
                                .unwrap();
                            graph.schema_updated = true;

                            let oe_col_size = graph.get_max_edge_offset(
                                src_label,
                                edge_label,
                                dst_label,
                                Direction::Outgoing,
                            );
                            let oe_prefix = format!(
                                "{}_oep_{}_{}_{}_col_{}",
                                graph.get_partition_prefix(),
                                src_label as usize,
                                edge_label as usize,
                                dst_label as usize,
                                idx
                            );
                            let oe_cb = create_column(column_data_type, oe_prefix.as_str(), oe_col_size);

                            let ie_col_size = graph.get_max_edge_offset(
                                dst_label,
                                edge_label,
                                src_label,
                                Direction::Incoming,
                            );
                            let ie_prefix = format!(
                                "{}_iep_{}_{}_{}_col_{}",
                                graph.get_partition_prefix(),
                                src_label as usize,
                                edge_label as usize,
                                dst_label as usize,
                                idx
                            );
                            let mut ie_cb =
                                create_column(column_data_type, ie_prefix.as_str(), ie_col_size);

                            ie_cb.set_column_batch(&offsets, &column.take_data());
                            column_builders
                                .insert((src_label, edge_label, dst_label, k.clone()), (ie_cb, oe_cb));
                        }
                    }
                }
            }
        }
    }
}

fn process_csv_rows<F>(path: &PathBuf, mut process_row: F, skip_header: bool, delim: u8)
    where
        F: FnMut(&csv::StringRecord),
{
    if let Some(path_str) = path.clone().to_str() {
        if path_str.ends_with(".csv.gz") {
            if let Ok(gz_reader) = GzReader::from_path(&path) {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(skip_header)
                    .from_reader(gz_reader);
                for result in rdr.records() {
                    if let Ok(record) = result {
                        process_row(&record);
                    }
                }
            }
        } else if path_str.ends_with(".csv") {
            if let Ok(file) = File::open(&path) {
                let reader = BufReader::new(file);
                let mut rdr = ReaderBuilder::new()
                    .delimiter(delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(skip_header)
                    .from_reader(reader);
                for result in rdr.records() {
                    if let Ok(record) = result {
                        process_row(&record);
                    }
                }
            }
        }
    }
}

fn collect_csv_rows(path: &PathBuf, skip_header: bool, delim: u8) -> Vec<csv::StringRecord> {
    let mut records = Vec::new();
    if let Some(path_str) = path.clone().to_str() {
        if path_str.ends_with(".csv.gz") {
            if let Ok(gz_reader) = GzReader::from_path(&path) {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(skip_header)
                    .from_reader(gz_reader);
                for result in rdr.records() {
                    if let Ok(record) = result {
                        records.push(record);
                    }
                }
            }
        } else if path_str.ends_with(".csv") {
            if let Ok(file) = File::open(&path) {
                let reader = BufReader::new(file);
                let mut rdr = ReaderBuilder::new()
                    .delimiter(delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(skip_header)
                    .from_reader(reader);
                for result in rdr.records() {
                    if let Ok(record) = result {
                        records.push(record);
                    }
                }
            }
        }
    }
    records
}

pub struct GraphModifier {
    input_dir: PathBuf,
    partitions: usize,
    delim: u8,
    skip_header: bool,
}

impl GraphModifier {
    pub fn new<D: AsRef<Path>>(input_dir: D) -> GraphModifier {
        Self { input_dir: input_dir.as_ref().to_path_buf(), partitions: 1, delim: b'|', skip_header: false }
    }

    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn partitions(&mut self, partitions: usize) {
        self.partitions = partitions;
    }

    pub fn skip_header(&mut self) {
        self.skip_header = true;
    }

    fn parallel_delete_impl<G, I>(
        &self, graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
        edge_file_strings: &Vec<String>, input_header: &[(String, DataType)],
    ) where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let graph_header = graph
            .graph_schema
            .get_edge_header(src_label, edge_label, dst_label);
        if graph_header.is_none() {
            return ();
        }

        let mut delete_edge_set = Vec::new();
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

        let is_src_static = graph.graph_schema.is_static_vertex(src_label);
        let is_dst_static = graph.graph_schema.is_static_vertex(dst_label);
        let edge_files = edge_files.unwrap();
        for edge_file in edge_files.iter() {
            process_csv_rows(
                edge_file,
                |record| {
                    let edge_meta = parser.parse_edge_meta(&record);
                    let mut keep_vertex = false;
                    if is_src_static && is_dst_static {
                        keep_vertex = true;
                    } else if is_src_static && !is_dst_static {
                        if edge_meta.dst_global_id.index() % self.partitions == graph.partition {
                            keep_vertex = true;
                        }
                    } else if !is_src_static && is_dst_static {
                        if edge_meta.src_global_id.index() % self.partitions == graph.partition {
                            keep_vertex = true;
                        }
                    } else if !is_src_static && !is_dst_static {
                        if edge_meta.src_global_id.index() % self.partitions == graph.partition
                            || edge_meta.dst_global_id.index() % self.partitions == graph.partition
                        {
                            keep_vertex = true;
                        }
                    }
                    if keep_vertex {
                        delete_edge_set.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                    }
                },
                self.skip_header,
                self.delim,
            );
        }
        if delete_edge_set.is_empty() {
            return ();
        }

        let index = graph.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
        if let Some(csr) = graph.oe.get_mut(&index) {
            let shuffle_indices = csr.delete_edges(&delete_edge_set, false, &graph.vertex_map);
            if let Some(table) = graph.oe_edge_prop_table.get_mut(&index) {
                table.parallel_move(&shuffle_indices);
            }
        }
        if let Some(csr) = graph.ie.get_mut(&index) {
            let shuffle_indices = csr.delete_edges(&delete_edge_set, true, &graph.vertex_map);
            if let Some(table) = graph.ie_edge_prop_table.get_mut(&index) {
                table.parallel_move(&shuffle_indices);
            }
        }
    }

    pub fn apply_edges_delete_with_filename<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
        filenames: &Vec<String>, src_id_col: i32, dst_id_col: i32,
    ) -> GDBResult<()>
        where
            G: FromStr + Send + Sync + IndexType + Eq,
            I: Send + Sync + IndexType,
    {
        let mut input_header: Vec<(String, DataType)> = vec![];
        input_header.resize(
            std::cmp::max(src_id_col as usize, dst_id_col as usize) + 1,
            ("".to_string(), DataType::NULL),
        );
        input_header[src_id_col as usize] = ("start_id".to_string(), DataType::ID);
        input_header[dst_id_col as usize] = ("end_id".to_string(), DataType::ID);
        self.parallel_delete_impl(graph, src_label, edge_label, dst_label, filenames, &input_header);

        Ok(())
    }

    pub fn apply_vertices_insert_with_filename<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, label: LabelId, filenames: &Vec<String>, id_col: i32,
        mappings: &Vec<i32>,
    ) -> GDBResult<()>
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        let start = Instant::now();
        let graph_header = graph
            .graph_schema
            .get_vertex_header(label as LabelId)
            .unwrap();
        let header = graph_header.to_vec();

        let parser = LDBCVertexParser::<G>::new(label as LabelId, id_col as usize);
        let vertex_files_prefix = self.input_dir.clone();

        let vertex_files = get_files_list(&vertex_files_prefix, filenames);
        if vertex_files.is_err() {
            warn!(
                "Get vertex files {:?}/{:?} failed: {:?}",
                &vertex_files_prefix,
                filenames,
                vertex_files.err().unwrap()
            );
            return Ok(());
        }
        let vertex_files = vertex_files.unwrap();
        if vertex_files.is_empty() {
            return Ok(());
        }
        let mut df = DataFrame::new(&header);
        let mut id_list = vec![];
        let t0 = start.elapsed().as_secs_f64();
        let start = Instant::now();
        for vertex_file in vertex_files.iter() {
            let records = collect_csv_rows(vertex_file, self.skip_header, self.delim);
            let (ids, props, corners): (Vec<G>, Vec<Vec<Item>>, Vec<G>) = records
                .par_iter()
                .fold(
                    || (Vec::new(), Vec::new(), Vec::new()),
                    |(mut id_vec, mut prop_vec, mut corner_vec), x| {
                        let vertex_meta = parser.parse_vertex_meta(&x);
                        if vertex_meta.global_id.index() % self.partitions == graph.partition {
                            id_vec.push(vertex_meta.global_id);
                            prop_vec.push(parse_properties_by_mappings(&x, &header, mappings).unwrap());
                        }
                        (id_vec, prop_vec, corner_vec)
                    },
                )
                .reduce(
                    || (Vec::new(), Vec::new(), Vec::new()),
                    |a, b| (vec![a.0, b.0].concat(), vec![a.1, b.1].concat(), vec![a.2, b.2].concat()),
                );
            id_list.extend(ids);
            // corner_id_list.extend(corners);
            df.append_rows(props);
        }
        let t1 = start.elapsed().as_secs_f64();
        let start = Instant::now();
        let offsets = graph
            .vertex_map
            .insert_native_vertices(label, id_list);
        let t2 = start.elapsed().as_secs_f64();
        let start = Instant::now();
        // graph
        //     .vertex_map
        //     .insert_corner_vertices(label, corner_id_list);
        let t3 = start.elapsed().as_secs_f64();
        let start = Instant::now();
        graph.vertex_prop_table[label as usize].resize(graph.vertex_map.indexers[label as usize].len());
        let t4 = start.elapsed().as_secs_f64();
        let start = Instant::now();
        graph.vertex_prop_table[label as usize].insert_batch(&offsets, &df);
        let t5 = start.elapsed().as_secs_f64();

        println!(
            "apply_vertices_insert_with_filename: t0: {}, t1: {}, t2: {}, t3: {}, t4: {}, t5: {}",
            t0, t1, t2, t3, t4, t5
        );

        Ok(())
    }

    pub fn apply_edges_insert_with_filename<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
        filenames: &Vec<String>, src_id_col: i32, dst_id_col: i32, mappings: &Vec<i32>,
    ) -> GDBResult<()>
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        let start = Instant::now();
        let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
        parser.with_endpoint_col_id(src_id_col as usize, dst_id_col as usize);

        let edge_files_prefix = self.input_dir.clone();
        let edge_files = get_files_list(&edge_files_prefix, filenames);
        if edge_files.is_err() {
            warn!(
                "Get vertex files {:?}/{:?} failed: {:?}",
                &edge_files_prefix,
                filenames,
                edge_files.err().unwrap()
            );
            return Ok(());
        }
        let edge_files = edge_files.unwrap();
        let graph_header = graph
            .graph_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let is_src_static = graph.graph_schema.is_static_vertex(src_label);
        let is_dst_static = graph.graph_schema.is_static_vertex(dst_label);
        let t0 = start.elapsed().as_secs_f64();
        let start = Instant::now();
        let (edges, prop_table) = if graph_header.is_empty() {
            let mut edges = vec![];
            for file in edge_files {
                process_csv_rows(
                    &file,
                    |record| {
                        let edge_meta = parser.parse_edge_meta(&record);
                        if is_src_static && is_dst_static {
                            edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                        } else if is_src_static && !is_dst_static {
                            if edge_meta.dst_global_id.index() % self.partitions == graph.partition {
                                edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                            }
                        } else if !is_src_static && is_dst_static {
                            if edge_meta.src_global_id.index() % self.partitions == graph.partition {
                                edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                            }
                        } else if !is_src_static && !is_dst_static {
                            if edge_meta.src_global_id.index() % self.partitions == graph.partition
                                || edge_meta.dst_global_id.index() % self.partitions == graph.partition
                            {
                                edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                            }
                        }
                    },
                    self.skip_header,
                    self.delim,
                );
            }
            (edges, None)
        } else {
            let mut edges = vec![];
            let mut prop_table = DataFrame::new(graph_header);
            for file in edge_files {
                process_csv_rows(
                    &file,
                    |record| {
                        let edge_meta = parser.parse_edge_meta(&record);
                        if is_src_static && is_dst_static {
                            edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                            if let Ok(properties) =
                                parse_properties_by_mappings(&record, &graph_header, mappings)
                            {
                                prop_table.append(properties);
                            }
                        } else if is_src_static && !is_dst_static {
                            if edge_meta.dst_global_id.index() % self.partitions == graph.partition {
                                edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                                if let Ok(properties) =
                                    parse_properties_by_mappings(&record, &graph_header, mappings)
                                {
                                    prop_table.append(properties);
                                }
                            }
                        } else if !is_src_static && is_dst_static {
                            if edge_meta.src_global_id.index() % self.partitions == graph.partition {
                                edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                                if let Ok(properties) =
                                    parse_properties_by_mappings(&record, &graph_header, mappings)
                                {
                                    prop_table.append(properties);
                                }
                            }
                        } else if !is_src_static && !is_dst_static {
                            if edge_meta.src_global_id.index() % self.partitions == graph.partition
                                || edge_meta.dst_global_id.index() % self.partitions == graph.partition
                            {
                                edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                                if let Ok(properties) =
                                    parse_properties_by_mappings(&record, &graph_header, mappings)
                                {
                                    prop_table.append(properties);
                                }
                            }
                        }
                    },
                    self.skip_header,
                    self.delim,
                )
            }
            (edges, Some(prop_table))
        };
        let t1 = start.elapsed().as_secs_f64();
        let start = Instant::now();

        let t2 = start.elapsed().as_secs_f64();
        let start = Instant::now();

        let t3 = start.elapsed().as_secs_f64();
        let start = Instant::now();

        let new_src_num = graph.vertex_map.vertex_num(src_label);

        let index = graph.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
        if let Some(csr) = graph.oe.get_mut(&index) {
            csr.insert_edges_beta(
                new_src_num,
                // &parsed_edges,
                &edges,
                prop_table.as_ref(),
                false,
                graph.oe_edge_prop_table.get_mut(&index), &graph.vertex_map, src_label,
            );
        }
        let t4 = start.elapsed().as_secs_f64();
        let start = Instant::now();

        let new_dst_num = graph.vertex_map.vertex_num(dst_label);
        if let Some(csr) = graph.ie.get_mut(&index) {
            csr.insert_edges_beta(
                new_dst_num,
                // &parsed_edges,
                &edges,
                prop_table.as_ref(),
                true,
                graph.ie_edge_prop_table.get_mut(&index), &graph.vertex_map, dst_label,
            );
        }
        let t5 = start.elapsed().as_secs_f64();
        println!("apply_edges_insert_with_filename: {}, {}, {}, {}, {}, {}", t0, t1, t2, t3, t4, t5);

        Ok(())
    }
}
