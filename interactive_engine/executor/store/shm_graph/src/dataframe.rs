use std::any::Any;
use std::fmt::Debug;

use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};

use crate::columns::{DataType, Item, RefItem};
use crate::types::DefaultId;

pub trait HeapColumn {
    fn get_type(&self) -> DataType;
    fn get(&self, index: usize) -> Option<RefItem>;
    fn set(&mut self, index: usize, val: Item);
    fn push(&mut self, val: Item);
    fn len(&self) -> usize;
    fn as_any(&self) -> &dyn Any;
}

pub struct I32HColumn {
    pub data: Vec<i32>,
}

unsafe impl Send for I32HColumn {}
unsafe impl Sync for I32HColumn {}

impl I32HColumn {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn from(data: Vec<i32>) -> I32HColumn {
        I32HColumn { data }
    }
}

impl HeapColumn for I32HColumn {
    fn get_type(&self) -> DataType {
        DataType::Int32
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::Int32(*x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::Int32(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0;
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::Int32(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct I64HColumn {
    pub data: Vec<i64>,
}

unsafe impl Send for I64HColumn {}
unsafe impl Sync for I64HColumn {}

impl I64HColumn {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn from(data: Vec<i64>) -> I64HColumn {
        I64HColumn { data }
    }
}

impl HeapColumn for I64HColumn {
    fn get_type(&self) -> DataType {
        DataType::Int64
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::Int64(*x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::Int64(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0;
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::Int64(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct U64HColumn {
    pub data: Vec<u64>,
}

unsafe impl Send for U64HColumn {}
unsafe impl Sync for U64HColumn {}

impl U64HColumn {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn from(data: Vec<u64>) -> U64HColumn {
        U64HColumn { data }
    }
}

impl HeapColumn for U64HColumn {
    fn get_type(&self) -> DataType {
        DataType::UInt64
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data
            .get(index)
            .map(|x| RefItem::UInt64(*x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::UInt64(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0;
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::UInt64(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct IDHColumn {
    pub data: Vec<DefaultId>,
}

unsafe impl Send for IDHColumn {}
unsafe impl Sync for IDHColumn {}

impl IDHColumn {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn from(data: Vec<DefaultId>) -> IDHColumn {
        IDHColumn { data }
    }
}

impl HeapColumn for IDHColumn {
    fn get_type(&self) -> DataType {
        DataType::ID
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data
            .get(index)
            .map(|x| RefItem::VertexId(*x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::VertexId(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0;
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::VertexId(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
pub struct ColumnMetadata {
    data: Box<dyn HeapColumn>,
    column_name: String,
    data_type: DataType,
}

fn read_column<R: ReadExt>(reader: &mut R) -> std::io::Result<Box<dyn HeapColumn>> {
    let data: Box<dyn HeapColumn> = match reader.read_u8()? {
        0 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = Vec::<i32>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(reader.read_i32()?);
            }
            Box::new(I32HColumn { data })
        }
        2 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = Vec::<i64>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(reader.read_i64()?);
            }
            Box::new(I64HColumn { data })
        }
        3 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = Vec::<u64>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(reader.read_u64()?);
            }
            Box::new(U64HColumn { data })
        }
        4 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = Vec::<usize>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(reader.read_u64()? as usize);
            }
            Box::new(IDHColumn { data })
        }
        _ => panic!("Unknown column type"),
    };
    Ok(data)
}

fn write_column<W: WriteExt>(column: &Box<dyn HeapColumn>, writer: &mut W) -> std::io::Result<()> {
    if let Some(int32_column) = column.as_any().downcast_ref::<I32HColumn>() {
        writer.write_u8(0);
        writer.write_u64(column.len() as u64);
        for i in int32_column.data.iter() {
            writer.write_i32(*i);
        }
    }
    if let Some(int64_column) = column.as_any().downcast_ref::<I64HColumn>() {
        writer.write_u8(2);
        writer.write_u64(column.len() as u64);
        for i in int64_column.data.iter() {
            writer.write_i64(*i);
        }
    }
    if let Some(uint64_column) = column.as_any().downcast_ref::<U64HColumn>() {
        writer.write_u8(3);
        writer.write_u64(column.len() as u64);
        for i in uint64_column.data.iter() {
            writer.write_u64(*i);
        }
    }
    if let Some(id_column) = column.as_any().downcast_ref::<IDHColumn>() {
        writer.write_u8(4);
        writer.write_u64(column.len() as u64);
        for i in id_column.data.iter() {
            writer.write_u64(*i as u64);
        }
    }
    Ok(())
}

fn clone_column(input: &Box<dyn HeapColumn>) -> Box<dyn HeapColumn> {
    if let Some(int32_column) = input.as_any().downcast_ref::<I32HColumn>() {
        Box::new(I32HColumn { data: int32_column.data.clone() })
    } else if let Some(int64_column) = input.as_any().downcast_ref::<I64HColumn>() {
        Box::new(I64HColumn { data: int64_column.data.clone() })
    } else if let Some(uint64_column) = input.as_any().downcast_ref::<U64HColumn>() {
        Box::new(U64HColumn { data: uint64_column.data.clone() })
    } else if let Some(id_column) = input.as_any().downcast_ref::<IDHColumn>() {
        Box::new(IDHColumn { data: id_column.data.clone() })
    } else {
        panic!("Unknown column type")
    }
}

impl Encode for ColumnMetadata {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        write_column(&self.data, writer)?;
        self.column_name.write_to(writer)?;
        self.data_type.write_to(writer)?;
        Ok(())
    }
}

impl Decode for ColumnMetadata {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let data: Box<dyn HeapColumn> = read_column(reader)?;
        let column_name = String::read_from(reader)?;
        let data_type = DataType::read_from(reader)?;
        Ok(ColumnMetadata { data, column_name, data_type })
    }
}

impl Clone for ColumnMetadata {
    fn clone(&self) -> Self {
        let data = clone_column(&self.data);
        ColumnMetadata { data, column_name: self.column_name.clone(), data_type: self.data_type.clone() }
    }
}

impl ColumnMetadata {
    pub fn new(data: Box<dyn HeapColumn>, column_name: String, data_type: DataType) -> Self {
        ColumnMetadata { data, column_name, data_type }
    }

    pub fn data(&self) -> &Box<dyn HeapColumn> {
        &self.data
    }

    pub fn take_data(&mut self) -> Box<dyn HeapColumn> {
        std::mem::replace(&mut self.data, Box::new(I32HColumn { data: vec![] }))
    }

    pub fn column_name(&self) -> String {
        self.column_name.clone()
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }
}

#[derive(Clone)]
pub struct DataFrame {
    columns: Vec<ColumnMetadata>,
}

impl Encode for DataFrame {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.columns.write_to(writer)?;
        Ok(())
    }
}

impl Decode for DataFrame {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let columns = Vec::<ColumnMetadata>::read_from(reader)?;
        Ok(DataFrame { columns })
    }
}

impl DataFrame {
    pub fn new_vertices_ids(data: Vec<u64>) -> Self {
        let columns =
            vec![ColumnMetadata::new(Box::new(U64HColumn { data }), "id".to_string(), DataType::ID)];
        DataFrame { columns }
    }

    pub fn new_edges_ids(data: Vec<usize>) -> Self {
        let columns =
            vec![ColumnMetadata::new(Box::new(IDHColumn { data }), "id".to_string(), DataType::ID)];
        DataFrame { columns }
    }

    pub fn add_column(&mut self, column: ColumnMetadata) {
        self.columns.push(column);
    }

    pub fn columns(&self) -> &Vec<ColumnMetadata> {
        &self.columns
    }

    pub fn take_columns(&mut self) -> Vec<ColumnMetadata> {
        std::mem::replace(&mut self.columns, Vec::new())
    }
}
