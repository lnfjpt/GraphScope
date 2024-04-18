use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io;
use std::io::Write;

use bmcsr::col_table::ColTable;
use bmcsr::columns::Item as GraphItem;
use dyn_type::CastError;
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};
use serde::{Deserialize, Serialize};

pub type DefaultId = usize;
pub type InternalId = usize;
pub type LabelId = u8;

pub enum WriteOp {
    InsertVertices {
        label: LabelId,
        global_ids: Vec<usize>,
        properties: Option<Vec<ArrayData>>,
    },
    InsertEdges {
        src_label: LabelId,
        edge_label: LabelId,
        dst_label: LabelId,
        edges: Vec<(usize, usize)>,
        properties: Option<Vec<ArrayData>>,
    },
    InsertVerticesBySchema {
        label: LabelId,
        input_dir: String,
        filenames: Vec<String>,
        id_col: i32,
        mappings: Vec<i32>,
    },
    InsertEdgesBySchema {
        src_label: LabelId,
        edge_label: LabelId,
        dst_label: LabelId,
        input_dir: String,
        filenames: Vec<String>,
        src_id_col: i32,
        dst_id_col: i32,
        mappings: Vec<i32>,
    },
    DeleteVertices {
        label: LabelId,
        global_ids: Vec<u64>,
    },
    DeleteEdges {
        src_label: LabelId,
        edge_label: LabelId,
        dst_label: LabelId,
        lids: Vec<(usize, usize)>,
    },
    DeleteVerticesBySchema {
        label: LabelId,
        input_dir: String,
        filenames: Vec<String>,
        id_col: i32,
    },
    DeleteEdgesBySchema {
        src_label: LabelId,
        edge_label: LabelId,
        dst_label: LabelId,
        input_dir: String,
        filenames: Vec<String>,
        src_id_col: i32,
        dst_id_col: i32,
    },
    SetVertices {
        label: LabelId,
        global_ids: Vec<usize>,
        properties: Vec<(String, ArrayData)>,
    },
    SetEdges {
        src_label: LabelId,
        edge_label: LabelId,
        dst_label: LabelId,
        src_offset: Vec<usize>,
        dst_offset: Vec<usize>,
        src_properties: Vec<(String, ArrayData)>,
        dst_properties: Vec<(String, ArrayData)>,
    },
}

impl Clone for WriteOp {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            WriteOp::InsertVertices { label, global_ids, properties } => WriteOp::InsertVertices {
                label: *label,
                global_ids: global_ids.clone(),
                properties: properties.clone(),
            },
            WriteOp::InsertEdges { src_label, edge_label, dst_label, edges, properties } => {
                WriteOp::InsertEdges {
                    src_label: *src_label,
                    edge_label: *edge_label,
                    dst_label: *dst_label,
                    edges: edges.clone(),
                    properties: properties.clone(),
                }
            }
            WriteOp::InsertVerticesBySchema { label, input_dir, filenames, id_col, mappings } => {
                WriteOp::InsertVerticesBySchema {
                    label: *label,
                    input_dir: input_dir.clone(),
                    filenames: filenames.clone(),
                    id_col: *id_col,
                    mappings: mappings.clone(),
                }
            }
            WriteOp::InsertEdgesBySchema {
                src_label,
                edge_label,
                dst_label,
                input_dir,
                filenames,
                src_id_col,
                dst_id_col,
                mappings,
            } => WriteOp::InsertEdgesBySchema {
                src_label: *src_label,
                edge_label: *edge_label,
                dst_label: *dst_label,
                input_dir: input_dir.clone(),
                filenames: filenames.clone(),
                src_id_col: *src_id_col,
                dst_id_col: *dst_id_col,
                mappings: mappings.clone(),
            },
            WriteOp::DeleteVertices { label, global_ids } => {
                WriteOp::DeleteVertices { label: *label, global_ids: global_ids.clone() }
            }
            WriteOp::DeleteEdges { src_label, edge_label, dst_label, lids } => WriteOp::DeleteEdges {
                src_label: *src_label,
                edge_label: *edge_label,
                dst_label: *dst_label,
                lids: lids.clone(),
            },
            WriteOp::DeleteVerticesBySchema { label, input_dir, filenames, id_col } => {
                WriteOp::DeleteVerticesBySchema {
                    label: *label,
                    input_dir: input_dir.clone(),
                    filenames: filenames.clone(),
                    id_col: *id_col,
                }
            }
            WriteOp::DeleteEdgesBySchema {
                src_label,
                edge_label,
                dst_label,
                input_dir,
                filenames,
                src_id_col,
                dst_id_col,
            } => WriteOp::DeleteEdgesBySchema {
                src_label: *src_label,
                edge_label: *edge_label,
                dst_label: *dst_label,
                input_dir: input_dir.clone(),
                filenames: filenames.clone(),
                src_id_col: *src_id_col,
                dst_id_col: *dst_id_col,
            },
            WriteOp::SetVertices { label, global_ids, properties } => WriteOp::SetVertices {
                label: *label,
                global_ids: global_ids.clone(),
                properties: properties.clone(),
            },
            WriteOp::SetEdges { src_label, edge_label, dst_label, src_offset, dst_offset, src_properties, dst_properties } => {
                WriteOp::SetEdges {
                    src_label: *src_label,
                    edge_label: *edge_label,
                    dst_label: *dst_label,
                    src_offset: src_offset.clone(),
                    dst_offset: dst_offset.clone(),
                    src_properties: src_properties.clone(),
                    dst_properties: dst_properties.clone(),
                }
            }
        }
    }
}

impl Encode for WriteOp {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            WriteOp::InsertVertices { label, global_ids, properties } => {
                writer.write_u8(*label);
                writer.write_u64(global_ids.len() as u64);
                for data in global_ids.iter() {
                    writer.write_u64(*data as u64);
                }
                if properties.is_none() {
                    writer.write_u64(0);
                } else {
                    if let Some(properties) = properties {
                        writer.write_u64(properties[0].len() as u64);
                        for data in properties.iter() {}
                    }
                }
            }
            WriteOp::InsertVerticesBySchema { label, input_dir, filenames, id_col, mappings } => {
                writer.write_u8(2);
                writer.write_u8(*label);
                input_dir.write_to(writer);
                writer.write_u64(filenames.len() as u64);
                for filename in filenames {
                    filename.write_to(writer);
                }
                writer.write_i32(*id_col);
                writer.write_u64(mappings.len() as u64);
                for index in mappings {
                    writer.write_i32(*index);
                }
            }
            WriteOp::InsertEdgesBySchema { src_label, edge_label, dst_label, input_dir, filenames, src_id_col, dst_id_col, mappings } => {
                writer.write_u8(3);
                writer.write_u8(*src_label);
                writer.write_u8(*edge_label);
                writer.write_u8(*dst_label);
                input_dir.write_to(writer);
                writer.write_u64(filenames.len() as u64);
                for filename in filenames {
                    filename.write_to(writer);
                }
                writer.write_i32(*src_id_col);
                writer.write_i32(*dst_id_col);
                writer.write_u64(mappings.len() as u64);
                for index in mappings {
                    writer.write_i32(*index);
                }
            }
            WriteOp::DeleteVerticesBySchema { label, input_dir, filenames, id_col } => {
                writer.write_u8(6);
                writer.write_u8(*label);
                input_dir.write_to(writer);
                writer.write_u64(filenames.len() as u64);
                for filename in filenames {
                    filename.write_to(writer);
                }
                writer.write_i32(*id_col);
            }
            WriteOp::DeleteEdgesBySchema { src_label, edge_label, dst_label, input_dir, filenames, src_id_col, dst_id_col } => {
                writer.write_u8(7);
                writer.write_u8(*src_label);
                writer.write_u8(*dst_label);
                input_dir.write_to(writer);
                writer.write_u64(filenames.len() as u64);
                for filename in filenames {
                    filename.write_to(writer);
                }
                writer.write_i32(*src_id_col);
                writer.write_i32(*dst_id_col);
            }
            WriteOp::SetVertices { label, global_ids, properties } => {
                writer.write_u8(8);
                writer.write_u8(*label);
                writer.write_u64(global_ids.len() as u64);
                for i in global_ids {
                    writer.write_u64(*i as u64);
                }
                properties.write_to(writer);
            }
            WriteOp::SetEdges { src_label, edge_label, dst_label, src_offset, dst_offset, src_properties, dst_properties } => {
                writer.write_u8(9);
                writer.write_u8(*src_label);
                writer.write_u8(*edge_label);
                writer.write_u8(*dst_label);
                writer.write_u64(src_offset.len() as u64);
                for i in src_offset {
                    writer.write_u64(*i as u64);
                }
                writer.write_u64(dst_offset.len() as u64);
                for i in dst_offset {
                    writer.write_u64(*i as u64);
                }
                src_properties.write_to(writer);
                dst_properties.write_to(writer);
            }
            _ => todo!(),
        }
        Ok(())
    }
}

impl Decode for WriteOp {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let op_type = reader.read_u8()?;
        match op_type {
            2 => {
                let label = reader.read_u8()?;
                let input_dir = String::read_from(reader)?;
                let files_num = reader.read_u64()? as usize;
                let mut filenames = Vec::with_capacity(files_num);
                for i in 0..files_num {
                    filenames.push(String::read_from(reader)?);
                }
                let id_col = reader.read_i32()?;
                let mappings_len = reader.read_u64()? as usize;
                let mut mappings = Vec::with_capacity(mappings_len);
                for i in 0..mappings_len {
                    mappings.push(reader.read_i32()?);
                }
                Ok(WriteOp::InsertVerticesBySchema { label, input_dir, filenames, id_col, mappings })
            }
            3 => {
                let src_label = reader.read_u8()?;
                let edge_label = reader.read_u8()?;
                let dst_label = reader.read_u8()?;
                let input_dir = String::read_from(reader)?;
                let files_num = reader.read_u64()? as usize;
                let mut filenames = Vec::with_capacity(files_num);
                for i in 0..files_num {
                    filenames.push(String::read_from(reader)?);
                }
                let src_id_col = reader.read_i32()?;
                let dst_id_col = reader.read_i32()?;
                let mappings_len = reader.read_u64()? as usize;
                let mut mappings = Vec::with_capacity(mappings_len);
                for i in 0..mappings_len {
                    mappings.push(reader.read_i32()?);
                }
                Ok(WriteOp::InsertEdgesBySchema { src_label, edge_label, dst_label, input_dir, filenames, src_id_col, dst_id_col, mappings })
            }
            6 => {
                let label = reader.read_u8()?;
                let input_dir = String::read_from(reader)?;
                let files_num = reader.read_u64()? as usize;
                let mut filenames = Vec::with_capacity(files_num);
                for i in 0..files_num {
                    filenames.push(String::read_from(reader)?);
                }
                let id_col = reader.read_i32()?;
                Ok(WriteOp::DeleteVerticesBySchema { label, input_dir, filenames, id_col })
            }
            7 => {
                let src_label = reader.read_u8()?;
                let edge_label = reader.read_u8()?;
                let dst_label = reader.read_u8()?;
                let input_dir = String::read_from(reader)?;
                let files_num = reader.read_u64()? as usize;
                let mut filenames = Vec::with_capacity(files_num);
                for i in 0..files_num {
                    filenames.push(String::read_from(reader)?);
                }
                let src_id_col = reader.read_i32()?;
                let dst_id_col = reader.read_i32()?;
                Ok(WriteOp::DeleteEdgesBySchema { src_label, edge_label, dst_label, input_dir, filenames, src_id_col, dst_id_col })
            }
            8 => {
                let label = reader.read_u8()?;
                let global_ids_len = reader.read_u64()? as usize;
                let mut global_ids = Vec::with_capacity(global_ids_len);
                for i in 0..global_ids_len {
                    global_ids.push(reader.read_u64()? as usize);
                }
                let properties = Vec::<(String, ArrayData)>::read_from(reader)?;
                Ok(WriteOp::SetVertices { label, global_ids, properties })
            }
            9 => {
                let src_label = reader.read_u8()?;
                let edge_label = reader.read_u8()?;
                let dst_label = reader.read_u8()?;
                let src_offset_len = reader.read_u64()? as usize;
                let mut src_offset = Vec::with_capacity(src_offset_len);
                for i in 0..src_offset_len {
                    src_offset.push(reader.read_u64()? as usize);
                }
                let dst_offset_len = reader.read_u64()? as usize;
                let mut dst_offset = Vec::with_capacity(dst_offset_len);
                for i in 0..dst_offset_len {
                    dst_offset.push(reader.read_u64()? as usize);
                }
                let src_properties = Vec::<(String, ArrayData)>::read_from(reader)?;
                let dst_properties = Vec::<(String, ArrayData)>::read_from(reader)?;
                Ok(WriteOp::SetEdges { src_label, edge_label, dst_label, src_offset, dst_offset, src_properties, dst_properties })
            }
            _ => todo!()
        }
    }
}

impl Debug for WriteOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriteOp InsertVertices")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    Int32,
    UInt32,
    Int64,
    UInt64,
    U64Vec,
    Double,
    ID,
    String,
    NULL,
}

impl<'a> From<&'a str> for DataType {
    fn from(_token: &'a str) -> Self {
        println!("token = {}", _token);
        let token_str = _token.to_uppercase();
        let token = token_str.as_str();
        if token == "LONG" {
            DataType::Int64
        } else if token == "INT32" {
            DataType::Int32
        } else if token == "DOUBLE" {
            DataType::Double
        } else if token == "ID" {
            DataType::ID
        } else {
            error!("Unsupported type {:?}", token);
            DataType::NULL
        }
    }
}

#[derive(Clone)]
pub enum Item {
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    U64Vec(Vec<u64>),
    Double(f64),
    ID(DefaultId),
    String(String),
    Null,
}

impl Item {
    #[inline]
    pub fn as_u64(&self) -> Result<u64, CastError> {
        match self {
            Item::Int32(v) => Ok(*v as u64),
            Item::UInt32(v) => Ok(*v as u64),
            Item::Int64(v) => Ok(*v as u64),
            Item::UInt64(v) => Ok(*v),
            Item::Double(v) => Ok(*v as u64),
            Item::ID(v) => Ok(*v as u64),
            _ => Ok(0_u64),
        }
    }

    #[inline]
    pub fn as_u64_vec(&self) -> Result<Vec<u64>, CastError> {
        match self {
            Item::U64Vec(v) => Ok(v.clone()),
            _ => Ok(Vec::<u64>::new()),
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Result<i32, CastError> {
        match self {
            Item::Int32(v) => Ok(*v),
            Item::UInt32(v) => Ok(*v as i32),
            Item::Int64(v) => Ok(*v as i32),
            Item::UInt64(v) => Ok(*v as i32),
            Item::Double(v) => Ok(*v as i32),
            Item::ID(v) => Ok(*v as i32),
            _ => Ok(0),
        }
    }
}

#[derive(Clone)]
pub enum RefItem<'a> {
    Int32(&'a i32),
    UInt32(&'a u32),
    Int64(&'a i64),
    UInt64(&'a u64),
    U64Vec(&'a Vec<u64>),
    Double(&'a f64),
    ID(&'a DefaultId),
    Null,
}

impl<'a> RefItem<'a> {
    pub fn to_owned(self) -> Item {
        match self {
            RefItem::Int32(v) => Item::Int32(*v),
            RefItem::UInt32(v) => Item::UInt32(*v),
            RefItem::Int64(v) => Item::Int64(*v),
            RefItem::UInt64(v) => Item::UInt64(*v),
            RefItem::U64Vec(v) => Item::U64Vec(v.clone()),
            RefItem::Double(v) => Item::Double(*v),
            RefItem::ID(v) => Item::ID(*v),
            RefItem::Null => Item::Null,
        }
    }
}

impl<'a> RefItem<'a> {
    #[inline]
    pub fn as_u64(&self) -> Result<u64, CastError> {
        match self {
            RefItem::Int32(v) => Ok(**v as u64),
            RefItem::UInt32(v) => Ok(**v as u64),
            RefItem::Int64(v) => Ok(**v as u64),
            RefItem::UInt64(v) => Ok(**v),
            RefItem::Double(v) => Ok(**v as u64),
            RefItem::ID(v) => Ok(**v as u64),
            _ => Ok(0_u64),
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Result<i32, CastError> {
        match self {
            RefItem::Int32(v) => Ok(**v),
            RefItem::UInt32(v) => Ok(**v as i32),
            RefItem::Int64(v) => Ok(**v as i32),
            RefItem::UInt64(v) => Ok(**v as i32),
            RefItem::Double(v) => Ok(**v as i32),
            RefItem::ID(v) => Ok(**v as i32),
            _ => Ok(0),
        }
    }
}

/// Edge direction.
#[derive(Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
#[repr(usize)]
pub enum Direction {
    /// An `Outgoing` edge is an outward edge *from* the current node.
    Outgoing = 0,
    /// An `Incoming` edge is an inbound edge *to* the current node.
    Incoming = 1,
}

impl Direction {
    /// Return the opposite `Direction`.
    #[inline]
    pub fn opposite(self) -> Direction {
        match self {
            Direction::Outgoing => Direction::Incoming,
            Direction::Incoming => Direction::Outgoing,
        }
    }

    /// Return `0` for `Outgoing` and `1` for `Incoming`.
    #[inline]
    pub fn index(self) -> usize {
        (self as usize) & 0x1
    }
}

impl Clone for Direction {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Clone)]
pub enum ArrayData {
    Int32Array(Vec<i32>),
    Uint64Array(Vec<u64>),
    UsizeArray(Vec<usize>),
}

impl Debug for ArrayData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArrayData::Int32Array(data) => write!(f, "Int32 data: {:?}", data),
            _ => write!(f, "Unknown data type"),
        }
    }
}

impl Encode for ArrayData {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            ArrayData::Int32Array(data) => {
                writer.write_u8(0);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    writer.write_i32(*i);
                }
            }
            _ => todo!()
        }
        Ok(())
    }
}

impl Decode for ArrayData {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let data_type = reader.read_u8()?;
        match data_type {
            0 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(reader.read_i32()?);
                }
                Ok(ArrayData::Int32Array(data))
            }
            _ => todo!()
        }
    }
}

impl ArrayData {
    pub fn as_ref(&self) -> ArrayDataRef {
        match self {
            ArrayData::Int32Array(data) => ArrayDataRef::Int32Array(&data),
            ArrayData::Uint64Array(data) => ArrayDataRef::Uint64Array(&data),
            _ => panic!("Unknown type"),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ArrayData::Int32Array(data) => data.len(),
            ArrayData::Uint64Array(data) => data.len(),
            ArrayData::UsizeArray(data) => data.len(),
        }
    }

    pub fn push_item(&mut self, item: GraphItem) {
        match self {
            ArrayData::Int32Array(data) => {
                if let GraphItem::Int32(item) = item {
                    data.push(item);
                }
            }
            ArrayData::Uint64Array(data) => {
                if let GraphItem::UInt64(item) = item {
                    data.push(item);
                }
            }
            ArrayData::UsizeArray(data) => panic!("Unknown type"),
        }
    }

    pub fn get_item(&self, index: usize) -> GraphItem {
        match self {
            ArrayData::Int32Array(data) => GraphItem::Int32(data[index]),
            ArrayData::Uint64Array(data) => GraphItem::UInt64(data[index]),
            ArrayData::UsizeArray(data) => panic!("Unknown type"),
        }
    }

    pub fn get_type(&self) -> DataType {
        match self {
            ArrayData::Int32Array(_) => DataType::Int32,
            ArrayData::Uint64Array(data) => DataType::UInt64,
            ArrayData::UsizeArray(data) => DataType::ID,
        }
    }
}

pub enum ArrayDataRef<'a> {
    Int32Array(&'a Vec<i32>),
    Uint64Array(&'a Vec<u64>),
}

pub enum GraphIndexError {
    UpdateFailure(String),
}

impl Debug for GraphIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GraphIndexError::UpdateFailure(msg) => write!(f, "Failed when update idnex: {}", msg),
        }
    }
}

impl Display for GraphIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for GraphIndexError {}

pub fn str_to_data_type(data_type_str: &String) -> DataType {
    match data_type_str.as_ref() {
        "Int32" => DataType::Int32,
        "UInt64" => DataType::UInt64,
        "String" => DataType::String,
        _ => DataType::NULL,
    }
}

pub fn str_to_default_value(default_value: &String, data_type: DataType) -> Item {
    match data_type {
        DataType::Int32 => Item::Int32(default_value.parse::<i32>().unwrap()),
        DataType::UInt64 => Item::UInt64(default_value.parse::<u64>().unwrap()),
        DataType::String => Item::String(default_value.clone()),
        _ => Item::Int32(0),
    }
}
