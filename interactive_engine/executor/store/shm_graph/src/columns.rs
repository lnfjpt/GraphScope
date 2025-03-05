use core::ops::Index;
use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use csv::StringRecord;
use dyn_type::object::RawType;
use dyn_type::CastError;
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use shm_container::{SharedStringVec, SharedVec};
use mmap_container::{MmapStringVec, MmapVec};

use crate::csr_trait::SafeMutPtr;
use crate::dataframe::*;
use crate::dataframe::{DateTimeHColumn, HeapColumn, I32HColumn, I64HColumn, StringHColumn};
use crate::date::{parse_date, Date};
use crate::date_time::{parse_datetime, DateTime};
use crate::error::GDBResult;
use crate::types::DefaultId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    Int32 = 1,
    UInt32 = 2,
    Int64 = 3,
    UInt64 = 4,
    Double = 5,
    String = 6,
    Date = 7,
    DateTime = 8,
    LCString = 9,
    ID = 10,
    NULL = 0,
}

impl Encode for DataType {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match *self {
            DataType::NULL => writer.write_u8(0)?,
            DataType::Int32 => writer.write_u8(1)?,
            DataType::UInt32 => writer.write_u8(2)?,
            DataType::Int64 => writer.write_u8(3)?,
            DataType::UInt64 => writer.write_u8(4)?,
            DataType::Double => writer.write_u8(5)?,
            DataType::String => writer.write_u8(6)?,
            DataType::Date => writer.write_u8(7)?,
            DataType::DateTime => writer.write_u8(8)?,
            DataType::LCString => writer.write_u8(9)?,
            DataType::ID => writer.write_u8(10)?,
        };
        Ok(())
    }
}

impl Decode for DataType {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let data_type = match reader.read_u8()? {
            0 => DataType::NULL,
            1 => DataType::Int32,
            2 => DataType::UInt32,
            3 => DataType::Int64,
            4 => DataType::UInt64,
            5 => DataType::Double,
            6 => DataType::String,
            7 => DataType::Date,
            8 => DataType::DateTime,
            9 => DataType::LCString,
            10 => DataType::ID,
            _ => panic!("Unknown data type"),
        };
        Ok(data_type)
    }
}

impl DataType {
    pub fn from_i32(n: i32) -> Option<Self> {
        match n {
            0 => Some(Self::NULL),
            1 => Some(Self::Int32),
            2 => Some(Self::UInt32),
            3 => Some(Self::Int64),
            4 => Some(Self::UInt64),
            5 => Some(Self::Double),
            6 => Some(Self::String),
            7 => Some(Self::Date),
            8 => Some(Self::DateTime),
            9 => Some(Self::LCString),
            10 => Some(Self::ID),
            _ => None,
        }
    }

    pub fn to_i32(&self) -> i32 {
        match self {
            Self::NULL => 0,
            Self::Int32 => 1,
            Self::UInt32 => 2,
            Self::Int64 => 3,
            Self::UInt64 => 4,
            Self::Double => 5,
            Self::String => 6,
            Self::Date => 7,
            Self::DateTime => 8,
            Self::LCString => 9,
            Self::ID => 10,
        }
    }
}

impl<'a> From<&'a str> for DataType {
    fn from(_token: &'a str) -> Self {
        info!("token = {}", _token);
        let token_str = _token.to_uppercase();
        let token = token_str.as_str();
        if token == "INT32" {
            DataType::Int32
        } else if token == "UINT32" {
            DataType::UInt32
        } else if token == "INT64" {
            DataType::Int64
        } else if token == "UINT64" {
            DataType::UInt64
        } else if token == "DOUBLE" {
            DataType::Double
        } else if token == "STRING" {
            DataType::String
        } else if token == "DATE" {
            DataType::Date
        } else if token == "DATETIME" {
            DataType::DateTime
        } else if token == "ID" {
            DataType::ID
        } else if token == "LCString" {
            DataType::LCString
        } else {
            error!("Unsupported type {:?}", token);
            DataType::NULL
        }
    }
}

#[derive(Clone)]
pub enum Item {
    Boolean(bool),
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    Float(f32),
    Double(f64),
    String(String),
    Date(Date),
    DateTime(DateTime),
    VertexId(usize),
    EdgeId((u64, u64)),
    Null,
}

#[derive(Clone)]
pub enum RefItem<'a> {
    Boolean(bool),
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    Float(f32),
    Double(f64),
    String(&'a str),
    Date(Date),
    DateTime(DateTime),
    VertexId(usize),
    EdgeId((u64, u64)),
    Null,
}

impl<'a> RefItem<'a> {
    pub fn to_owned(self) -> Item {
        match self {
            RefItem::Boolean(v) => Item::Boolean(v),
            RefItem::Int32(v) => Item::Int32(v),
            RefItem::UInt32(v) => Item::UInt32(v),
            RefItem::Int64(v) => Item::Int64(v),
            RefItem::UInt64(v) => Item::UInt64(v),
            RefItem::Float(v) => Item::Float(v),
            RefItem::Double(v) => Item::Double(v),
            RefItem::Date(v) => Item::Date(v),
            RefItem::DateTime(v) => Item::DateTime(v),
            RefItem::VertexId(v) => Item::VertexId(v),
            RefItem::EdgeId((src, dst)) => Item::EdgeId((src, dst)),
            RefItem::String(v) => Item::String(v.to_string()),
            RefItem::Null => Item::Null,
        }
    }
}

pub trait ConvertItem {
    fn to_ref_item(&self) -> RefItem;
    fn from_item(v: Item) -> Self;
}

impl ConvertItem for i32 {
    fn to_ref_item(&self) -> RefItem {
        RefItem::Int32(*self)
    }

    fn from_item(v: Item) -> Self {
        match v {
            Item::Int32(v) => v,
            _ => 0,
        }
    }
}

impl ConvertItem for DateTime {
    fn to_ref_item(&self) -> RefItem {
        RefItem::DateTime(*self)
    }

    fn from_item(v: Item) -> Self {
        match v {
            Item::DateTime(v) => v,
            _ => DateTime::empty(),
        }
    }
}

impl ConvertItem for () {
    fn to_ref_item(&self) -> RefItem {
        RefItem::Null
    }

    fn from_item(_v: Item) -> Self {
        ()
    }
}

impl Debug for Item {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Item::Int32(v) => {
                write!(f, "int32[{}]", v)
            }
            Item::UInt32(v) => {
                write!(f, "uint32[{}]", v)
            }
            Item::Int64(v) => {
                write!(f, "int64[{}]", v)
            }
            Item::UInt64(v) => {
                write!(f, "uint64[{}]", v)
            }
            Item::Double(v) => {
                write!(f, "double[{}]", v)
            }
            Item::Date(v) => {
                write!(f, "date[{}]", v.to_string())
            }
            Item::DateTime(v) => {
                write!(f, "datetime[{}]", v.to_string())
            }
            Item::VertexId(v) => {
                write!(f, "id[{}]", v)
            }
            Item::String(v) => {
                write!(f, "string[{}]", v)
            }
            _ => {
                write!(f, "")
            }
        }
    }
}

impl ToString for Item {
    fn to_string(&self) -> String {
        match self {
            Item::Int32(v) => v.to_string(),
            Item::UInt32(v) => v.to_string(),
            Item::Int64(v) => v.to_string(),
            Item::UInt64(v) => v.to_string(),
            Item::Double(v) => v.to_string(),
            Item::Date(v) => v.to_string(),
            Item::DateTime(v) => v.to_string(),
            Item::VertexId(v) => v.to_string(),
            Item::String(v) => v.to_string(),
            _ => "".to_string(),
        }
    }
}

impl<'a> Debug for RefItem<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RefItem::Int32(v) => {
                write!(f, "int32[{}]", v)
            }
            RefItem::UInt32(v) => {
                write!(f, "uint32[{}]", v)
            }
            RefItem::Int64(v) => {
                write!(f, "int64[{}]", v)
            }
            RefItem::UInt64(v) => {
                write!(f, "uint64[{}]", v)
            }
            RefItem::Double(v) => {
                write!(f, "double[{}]", v)
            }
            RefItem::Date(v) => {
                write!(f, "date[{}]", v.to_string())
            }
            RefItem::DateTime(v) => {
                write!(f, "datetime[{}]", v.to_string())
            }
            RefItem::VertexId(v) => {
                write!(f, "id[{}]", v)
            }
            RefItem::String(v) => {
                write!(f, "string[{}]", v)
            }
            _ => {
                write!(f, "")
            }
        }
    }
}

impl<'a> ToString for RefItem<'a> {
    fn to_string(&self) -> String {
        match self {
            RefItem::Int32(v) => v.to_string(),
            RefItem::UInt32(v) => v.to_string(),
            RefItem::Int64(v) => v.to_string(),
            RefItem::UInt64(v) => v.to_string(),
            RefItem::Double(v) => v.to_string(),
            RefItem::Date(v) => v.to_string(),
            RefItem::DateTime(v) => v.to_string(),
            RefItem::VertexId(v) => v.to_string(),
            RefItem::String(v) => v.to_string(),
            _ => "".to_string(),
        }
    }
}

impl<'a> RefItem<'a> {
    #[inline]
    pub fn as_u64(&self) -> Result<u64, CastError> {
        match self {
            RefItem::Int32(v) => Ok(*v as u64),
            RefItem::UInt32(v) => Ok(*v as u64),
            RefItem::Int64(v) => Ok(*v as u64),
            RefItem::UInt64(v) => Ok(*v),
            RefItem::Double(v) => Ok(*v as u64),
            RefItem::Date(_) => Ok(0_u64),
            RefItem::DateTime(v) => Ok(v.to_i64() as u64),
            RefItem::VertexId(v) => Ok(*v as u64),
            RefItem::String(_) => Err(CastError::new::<u64>(RawType::String)),
            _ => Ok(0_u64),
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Result<i32, CastError> {
        match self {
            RefItem::Int32(v) => Ok(*v),
            RefItem::UInt32(v) => Ok(*v as i32),
            RefItem::Int64(v) => Ok(*v as i32),
            RefItem::UInt64(v) => Ok(*v as i32),
            RefItem::Double(v) => Ok(*v as i32),
            RefItem::Date(_) => Ok(0),
            RefItem::DateTime(_) => Ok(0),
            RefItem::VertexId(v) => Ok(*v as i32),
            RefItem::String(_) => Err(CastError::new::<i32>(RawType::String)),
            _ => Ok(0),
        }
    }

    #[inline]
    pub fn as_str(&self) -> Result<Cow<'_, str>, CastError> {
        match self {
            RefItem::String(str) => Ok(Cow::Borrowed(*str)),
            _ => Err(CastError::new::<String>(RawType::Unknown)),
        }
    }

    #[inline]
    pub fn as_datetime(&self) -> Result<DateTime, CastError> {
        match self {
            RefItem::Int32(_) => Err(CastError::new::<u64>(RawType::Integer)),
            RefItem::UInt32(_) => Err(CastError::new::<u64>(RawType::Integer)),
            RefItem::Int64(_) => Err(CastError::new::<u64>(RawType::Long)),
            RefItem::UInt64(_) => Err(CastError::new::<u64>(RawType::Long)),
            RefItem::Double(_) => Err(CastError::new::<u64>(RawType::Float)),
            RefItem::Date(_) => Err(CastError::new::<u64>(RawType::Unknown)),
            RefItem::DateTime(v) => Ok(*v),
            RefItem::VertexId(_) => Err(CastError::new::<u64>(RawType::Long)),
            RefItem::String(_) => Err(CastError::new::<u64>(RawType::String)),
            _ => Err(CastError::new::<u64>(RawType::Unknown)),
        }
    }
}

pub fn parse_properties_by_mappings(
    record: &StringRecord, header: &[(String, DataType, bool)], mappings: &Vec<i32>,
) -> GDBResult<Vec<Item>> {
    let mut properties = vec![];
    for (index, val) in record.iter().enumerate() {
        if index < mappings.len() && mappings[index] >= 0 {
            match header[mappings[index] as usize].1 {
                DataType::Int32 => {
                    properties.push(Item::Int32(val.parse::<i32>()?));
                }
                DataType::UInt32 => {
                    properties.push(Item::UInt32(val.parse::<u32>()?));
                }
                DataType::Int64 => {
                    properties.push(Item::Int64(val.parse::<i64>()?));
                }
                DataType::UInt64 => {
                    properties.push(Item::UInt64(val.parse::<u64>()?));
                }
                DataType::String => {
                    properties.push(Item::String(val.to_string()));
                }
                DataType::Date => {
                    properties.push(Item::Date(parse_date(val)?));
                }
                DataType::DateTime => {
                    properties.push(Item::DateTime(parse_datetime(val)));
                }
                DataType::Double => {
                    properties.push(Item::Double(val.parse::<f64>()?));
                }
                DataType::NULL => {
                    error!("Unexpected field type");
                }
                DataType::ID => {}
                DataType::LCString => {
                    properties.push(Item::String(val.to_string()));
                }
            }
        }
    }
    Ok(properties)
}

pub trait Column {
    fn get_type(&self) -> DataType;
    fn get(&self, index: usize) -> Option<RefItem>;
    fn len(&self) -> usize;

    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>);
    fn resize(&mut self, new_size: usize);

    fn set(&mut self, index: usize, val: Item);
    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>);

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    );

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]);
}

pub struct NullColumn {
    size: usize,
}

unsafe impl Send for NullColumn {}

unsafe impl Sync for NullColumn {}

impl NullColumn {
    pub fn new(size: usize) -> Self {
        Self { size }
    }
}

impl Column for NullColumn {
    fn get_type(&self) -> DataType {
        DataType::NULL
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.size {
            Some(RefItem::Null)
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.size
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {}

    fn resize(&mut self, new_size: usize) {
        self.size = new_size;
    }

    fn set(&mut self, index: usize, val: Item) {}

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {}

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {}

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {}
}

pub struct Int32Column {
    pub data: SharedVec<i32>,
}

unsafe impl Send for Int32Column {}

unsafe impl Sync for Int32Column {}

impl Int32Column {
    pub fn create(name: &str, len: usize) -> Self {
        Self { data: SharedVec::<i32>::create(name, len) }
    }

    pub fn load(path: &str, name: &str) {
        SharedVec::<i32>::load(path, name);
    }

    pub fn open(path: &str) -> Self {
        Self { data: SharedVec::<i32>::open(path) }
    }
}

impl Column for Int32Column {
    fn get_type(&self) -> DataType {
        DataType::Int32
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.data.len() {
            Some(RefItem::Int32(self.data[index]))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        self.data.parallel_move(indices);
    }

    fn resize(&mut self, new_size: usize) {
        self.data.resize(new_size);
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

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        if col.as_any().is::<I32HColumn>() {
            let casted_col = col
                .as_any()
                .downcast_ref::<I32HColumn>()
                .unwrap();
            let mut_self_data = SafeMutPtr::new(&mut self.data);
            index
                .par_iter()
                .enumerate()
                .for_each(|(idx, val)| {
                    if *val != usize::MAX {
                        assert!(*val < self.data.len());
                        mut_self_data.get_mut()[*val] = casted_col.data[idx];
                    }
                });
        }
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        self.data
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        self.data
            .inplace_parallel_range_move(new_size, range_diff);
    }
}

pub struct UInt32Column {
    pub data: SharedVec<u32>,
}

impl UInt32Column {
    pub fn create(name: &str, len: usize) -> Self {
        Self { data: SharedVec::<u32>::create(name, len) }
    }

    pub fn load(path: &str, name: &str) {
        SharedVec::<u32>::load(path, name);
    }

    pub fn open(path: &str) -> Self {
        Self { data: SharedVec::<u32>::open(path) }
    }
}

unsafe impl Send for UInt32Column {}

unsafe impl Sync for UInt32Column {}

impl Column for UInt32Column {
    fn get_type(&self) -> DataType {
        DataType::UInt32
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.data.len() {
            Some(RefItem::UInt32(self.data[index]))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        self.data.parallel_move(indices);
    }

    fn resize(&mut self, new_size: usize) {
        self.data.resize(new_size);
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::UInt32(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0;
            }
        }
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        panic!("not implemented...");
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        self.data
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        self.data
            .inplace_parallel_range_move(new_size, range_diff);
    }
}

pub struct Int64Column {
    pub data: SharedVec<i64>,
}

impl Int64Column {
    pub fn create(name: &str, len: usize) -> Self {
        Self { data: SharedVec::<i64>::create(name, len) }
    }

    pub fn load(path: &str, name: &str) {
        SharedVec::<i64>::load(path, name);
    }

    pub fn open(path: &str) -> Self {
        Self { data: SharedVec::<i64>::open(path) }
    }
}

unsafe impl Send for Int64Column {}

unsafe impl Sync for Int64Column {}

impl Column for Int64Column {
    fn get_type(&self) -> DataType {
        DataType::Int64
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.data.len() {
            Some(RefItem::Int64(self.data[index]))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        self.data.parallel_move(indices);
    }

    fn resize(&mut self, new_size: usize) {
        self.data.resize(new_size);
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

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        if col.as_any().is::<I64HColumn>() {
            let casted_col = col
                .as_any()
                .downcast_ref::<I64HColumn>()
                .unwrap();
            let mut_self_data = SafeMutPtr::new(&mut self.data);
            index
                .par_iter()
                .enumerate()
                .for_each(|(idx, val)| {
                    if *val != usize::MAX {
                        assert!(*val < self.data.len());
                        mut_self_data.get_mut()[*val] = casted_col.data[idx];
                    }
                });
        }
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        self.data
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        self.data
            .inplace_parallel_range_move(new_size, range_diff);
    }
}

pub struct UInt64Column {
    pub data: SharedVec<u64>,
}

impl UInt64Column {
    pub fn create(name: &str, len: usize) -> Self {
        Self { data: SharedVec::<u64>::create(name, len) }
    }

    pub fn load(path: &str, name: &str) {
        SharedVec::<u64>::load(path, name);
    }

    pub fn open(path: &str) -> Self {
        Self { data: SharedVec::<u64>::open(path) }
    }
}

unsafe impl Send for UInt64Column {}

unsafe impl Sync for UInt64Column {}

impl Column for UInt64Column {
    fn get_type(&self) -> DataType {
        DataType::UInt64
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.data.len() {
            Some(RefItem::UInt64(self.data[index]))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        self.data.parallel_move(indices);
    }

    fn resize(&mut self, new_size: usize) {
        self.data.resize(new_size);
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

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        if col.as_any().is::<U64HColumn>() {
            let casted_col = col
                .as_any()
                .downcast_ref::<U64HColumn>()
                .unwrap();
            let mut_self_data = SafeMutPtr::new(&mut self.data);
            index
                .par_iter()
                .enumerate()
                .for_each(|(idx, val)| {
                    if *val != usize::MAX {
                        assert!(*val < self.data.len());
                        mut_self_data.get_mut()[*val] = casted_col.data[idx];
                    }
                });
        }
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        self.data
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        self.data
            .inplace_parallel_range_move(new_size, range_diff);
    }
}

pub struct IDColumn {
    pub data: SharedVec<DefaultId>,
}

impl IDColumn {
    pub fn create(name: &str, len: usize) -> Self {
        Self { data: SharedVec::<DefaultId>::create(name, len) }
    }

    pub fn load(path: &str, name: &str) {
        SharedVec::<DefaultId>::load(path, name);
    }

    pub fn open(path: &str) -> Self {
        Self { data: SharedVec::<DefaultId>::open(path) }
    }
}

unsafe impl Send for IDColumn {}

unsafe impl Sync for IDColumn {}

impl Column for IDColumn {
    fn get_type(&self) -> DataType {
        DataType::ID
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.data.len() {
            Some(RefItem::VertexId(self.data[index]))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        self.data.parallel_move(indices);
    }

    fn resize(&mut self, new_size: usize) {
        self.data.resize(new_size);
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

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        if col.as_any().is::<IDHColumn>() {
            let casted_col = col
                .as_any()
                .downcast_ref::<IDHColumn>()
                .unwrap();
            let mut_self_data = SafeMutPtr::new(&mut self.data);
            index
                .par_iter()
                .enumerate()
                .for_each(|(idx, val)| {
                    if *val != usize::MAX {
                        assert!(*val < self.data.len());
                        mut_self_data.get_mut()[*val] = casted_col.data[idx];
                    }
                });
        }
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        self.data
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        self.data
            .inplace_parallel_range_move(new_size, range_diff);
    }
}

pub struct DoubleColumn {
    pub data: SharedVec<f64>,
}

impl DoubleColumn {
    pub fn create(name: &str, len: usize) -> Self {
        Self { data: SharedVec::<f64>::create(name, len) }
    }

    pub fn load(path: &str, name: &str) {
        SharedVec::<f64>::load(path, name);
    }

    pub fn open(path: &str) -> Self {
        Self { data: SharedVec::<f64>::open(path) }
    }
}

unsafe impl Send for DoubleColumn {}

unsafe impl Sync for DoubleColumn {}

impl Column for DoubleColumn {
    fn get_type(&self) -> DataType {
        DataType::Double
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.data.len() {
            Some(RefItem::Double(self.data[index]))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        self.data.parallel_move(indices);
    }

    fn resize(&mut self, new_size: usize) {
        self.data.resize(new_size);
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::Double(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0_f64;
            }
        }
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        panic!("not implemented...");
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        self.data
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        self.data
            .inplace_parallel_range_move(new_size, range_diff);
    }
}

pub struct StringColumn {
    pub data: SharedStringVec,
}

impl StringColumn {
    pub fn load(path: &str, name: &str) {
        SharedStringVec::load(path, name);
    }

    pub fn open(path: &str) -> Self {
        Self { data: SharedStringVec::open(path) }
    }
}

unsafe impl Send for StringColumn {}

unsafe impl Sync for StringColumn {}

impl Column for StringColumn {
    fn get_type(&self) -> DataType {
        DataType::String
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::String(x))
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        self.data.parallel_move(indices);
    }

    fn resize(&mut self, new_size: usize) {
        self.data.resize(new_size);
    }

    fn set(&mut self, index: usize, val: Item) {
        panic!("not implemented...");
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        if col.as_any().is::<StringHColumn>() {
            let casted_col = col
                .as_any()
                .downcast_ref::<StringHColumn>()
                .unwrap();
            self.data.batch_set(index, &casted_col.data);
        }
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        self.data
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        self.data
            .inplace_parallel_range_move(new_size, range_diff);
    }
}

pub struct LowUsageStringColumn {
    pub data: MmapStringVec,
    pub append_data: SharedStringVec,
}

impl LowUsageStringColumn {
    pub fn load(path: &str, name: &str) {
        SharedStringVec::create(name, 0);
    }

    pub fn open(mmap_path: &str, shm_path: &str) -> Self {
        let data = MmapStringVec::open(mmap_path).expect("Failed to open LowUsageStringColumn");
        let append_data = SharedStringVec::open(shm_path);
        Self { data, append_data }
    }
}

unsafe impl Send for LowUsageStringColumn {}

unsafe impl Sync for LowUsageStringColumn {}

impl Column for LowUsageStringColumn {
    fn get_type(&self) -> DataType {
        DataType::String
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.data.len() {
            self.data.get(index).map(|x| RefItem::String(x))
        } else if index < self.data.len() + self.append_data.len() {
            self.append_data.get(index - self.data.len()).map(|x| RefItem::String(x))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len() + self.append_data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        panic!("not implemented...");
    }

    fn resize(&mut self, new_size: usize) {
        if new_size > self.data.len() {
            self.append_data.resize(new_size - self.data.len());
        } else {
            self.data.resize(new_size);
            self.append_data.resize(0);
        }
    }

    fn set(&mut self, index: usize, val: Item) {
        panic!("not implemented...");
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        if col.as_any().is::<StringHColumn>() {
            let casted_col = col
                .as_any()
                .downcast_ref::<StringHColumn>()
                .unwrap();
            let mut new_index = vec![];
            for i in index.iter() {
                assert!(*i >= self.data.len());
                new_index.push(*i - self.data.len());
            }
            self.append_data.batch_set(&new_index, &casted_col.data);
        }
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        panic!("not implemented...");
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        panic!("not implemented...");
    }
}

pub struct LCStringColumn {
    pub index: SharedVec<u16>,
    pub data: SharedStringVec,
    pub table: HashMap<String, u16>,
}

impl LCStringColumn {
    pub fn load(path: &str, name: &str) {
        SharedVec::<u16>::load(format!("{}_index", path).as_str(), format!("{}_index", name).as_str());
        SharedStringVec::load(format!("{}_data", path).as_str(), format!("{}_data", name).as_str());
    }

    pub fn open(path: &str) -> Self {
        let data = SharedStringVec::open(format!("{}_data", path).as_str());
        let mut table = HashMap::new();
        let len = data.len();
        for i in 0..len {
            table.insert(data.get_unchecked(i).to_string(), i as u16);
        }
        Self { index: SharedVec::<u16>::open(format!("{}_index", path).as_str()), data, table }
    }

    pub fn get_index(&self, content: &String) -> Option<usize> {
        for i in 0..self.data.len() {
            if self.data.get_unchecked(i) == content.as_str() {
                return Some(i);
            }
        }
        None
    }
}

unsafe impl Send for LCStringColumn {}

unsafe impl Sync for LCStringColumn {}

impl Column for LCStringColumn {
    fn get_type(&self) -> DataType {
        DataType::LCString
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.index.len() {
            Some(RefItem::String(
                self.data
                    .get_unchecked(self.index[index] as usize),
            ))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.index.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        self.index.parallel_move(indices);
    }

    fn resize(&mut self, new_size: usize) {
        self.index.resize(new_size);
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::String(v) => {
                let value = self.table.get(&v).unwrap();
                self.index[index] = *value;
            }
            _ => {
                self.index[index] = 0;
            }
        }
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        if col.as_any().is::<StringHColumn>() {
            let casted_col = col
                .as_any()
                .downcast_ref::<StringHColumn>()
                .unwrap();
            let mut_self_data = SafeMutPtr::new(&mut self.index);
            index
                .par_iter()
                .enumerate()
                .for_each(|(idx, val)| {
                    if *val != usize::MAX {
                        assert!(*val < self.index.len());
                        let value = self.table.get(&casted_col.data[idx]).unwrap();
                        mut_self_data.get_mut()[*val] = *value;
                    }
                });
        }
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        self.index
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        self.index
            .inplace_parallel_range_move(new_size, range_diff);
    }
}

impl Index<usize> for LCStringColumn {
    type Output = str;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        let idx = self.index[index];
        self.data.get_unchecked(idx as usize)
    }
}

pub struct LowUsageLCStringColumn {
    pub index: MmapVec<u16>,
    pub append_index: SharedVec<u16>,
    pub data: MmapStringVec,
    pub table: HashMap<String, u16>,
}

impl LowUsageLCStringColumn {
    pub fn load(path: &str, name: &str) {
        SharedVec::<u16>::create(format!("{}_index", name).as_str(), 0);
        SharedStringVec::create(format!("{}_data", name).as_str(), 0);
    }

    pub fn open(mmap_path: &str, shm_path: &str) -> Self {
        let data = MmapStringVec::open(format!("{}_data", mmap_path).as_str()).expect("Failed to open data of LowUsageLCStringColumn");
        let mut table = HashMap::new();
        let len = data.len();
        for i in 0..len {
            table.insert(data.get_unchecked(i).to_string(), i as u16);
        }
        let index = MmapVec::<u16>::open(format!("{}_index", mmap_path).as_str()).expect("Failed to open index of LowUsageLCStringColumn");
        let append_index = SharedVec::<u16>::open(format!("{}_index", shm_path).as_str());
        Self { index, append_index, data, table }
    }

    pub fn get_index(&self, content: &String) -> Option<usize> {
        for i in 0..self.data.len() {
            if self.data.get_unchecked(i) == content.as_str() {
                return Some(i);
            }
        }
        None
    }
}

unsafe impl Send for LowUsageLCStringColumn {}

unsafe impl Sync for LowUsageLCStringColumn {}

impl Column for LowUsageLCStringColumn {
    fn get_type(&self) -> DataType {
        DataType::LCString
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.index.len() {
            Some(RefItem::String(
                self.data
                    .get_unchecked(self.index[index] as usize),
            ))
        } else if index < self.index.len() + self.append_index.len() {
            Some(RefItem::String(
                self.data
                    .get_unchecked(self.append_index[index - self.index.len()] as usize),
            ))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.index.len() + self.append_index.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        panic!("not implemented...");
    }

    fn resize(&mut self, new_size: usize) {
        if new_size >= self.index.len() {
            self.append_index.resize(new_size - self.index.len());
        } else {
            self.index.resize(new_size);
            self.append_index.resize(0);
        }
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::String(v) => {
                let value = self.table.get(&v).unwrap();
                self.index[index] = *value;
            }
            _ => {
                self.index[index] = 0;
            }
        }
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        if col.as_any().is::<StringHColumn>() {
            let casted_col = col
                .as_any()
                .downcast_ref::<StringHColumn>()
                .unwrap();
            let mut_self_data = SafeMutPtr::new(&mut self.append_index);
            index
                .par_iter()
                .enumerate()
                .for_each(|(idx, val)| {
                    if *val != usize::MAX {
                        assert!(*val < self.index.len() + self.append_index.len());
                        let value = self.table.get(&casted_col.data[idx]).unwrap();
                        mut_self_data.get_mut()[*val - self.index.len()] = *value;
                    }
                });
        }
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        panic!("not implemented...");
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        panic!("not implemented...");
    }
}

impl Index<usize> for LowUsageLCStringColumn {
    type Output = str;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        let idx = self.index[index];
        self.data.get_unchecked(idx as usize)
    }
}

pub struct DateColumn {
    pub data: SharedVec<Date>,
}

impl DateColumn {
    pub fn create(name: &str, len: usize) -> Self {
        Self { data: SharedVec::<Date>::create(name, len) }
    }

    pub fn load(path: &str, name: &str) {
        SharedVec::<Date>::load(path, name);
    }

    pub fn open(path: &str) -> Self {
        Self { data: SharedVec::<Date>::open(path) }
    }
}

unsafe impl Send for DateColumn {}

unsafe impl Sync for DateColumn {}

impl Column for DateColumn {
    fn get_type(&self) -> DataType {
        DataType::Date
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.data.len() {
            Some(RefItem::Date(self.data[index]))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        self.data.parallel_move(indices);
    }

    fn resize(&mut self, new_size: usize) {
        self.data.resize(new_size);
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::Date(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = Date::empty();
            }
        }
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        if col.as_any().is::<DateHColumn>() {
            let casted_col = col
                .as_any()
                .downcast_ref::<DateHColumn>()
                .unwrap();
            let mut_self_data = SafeMutPtr::new(&mut self.data);
            index
                .par_iter()
                .enumerate()
                .for_each(|(idx, val)| {
                    if *val < usize::MAX {
                        assert!(*val < self.data.len());
                        mut_self_data.get_mut()[*val] = casted_col.data[idx];
                    }
                });
        }
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        self.data
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        self.data
            .inplace_parallel_range_move(new_size, range_diff);
    }
}

pub struct DateTimeColumn {
    pub data: SharedVec<DateTime>,
}

impl DateTimeColumn {
    pub fn create(name: &str, len: usize) -> Self {
        Self { data: SharedVec::<DateTime>::create(name, len) }
    }

    pub fn load(path: &str, name: &str) {
        SharedVec::<DateTime>::load(path, name);
    }

    pub fn open(path: &str) -> Self {
        Self { data: SharedVec::<DateTime>::open(path) }
    }
}

unsafe impl Send for DateTimeColumn {}

unsafe impl Sync for DateTimeColumn {}

impl Column for DateTimeColumn {
    fn get_type(&self) -> DataType {
        DataType::DateTime
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        if index < self.data.len() {
            Some(RefItem::DateTime(self.data[index]))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn reshuffle(&mut self, indices: &Vec<(usize, usize)>) {
        self.data.parallel_move(indices);
    }

    fn resize(&mut self, new_size: usize) {
        self.data.resize(new_size);
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::DateTime(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = DateTime::empty();
            }
        }
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn HeapColumn>) {
        if col.as_any().is::<DateTimeHColumn>() {
            let casted_col = col
                .as_any()
                .downcast_ref::<DateTimeHColumn>()
                .unwrap();
            let mut_self_data = SafeMutPtr::new(&mut self.data);
            index.par_iter().enumerate().for_each(|(idx, val)| {
                if *val != usize::MAX {
                    assert!(*val < self.data.len());
                    mut_self_data.get_mut()[*val] = casted_col.data[idx];
                }
            });
        }
    }

    fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        self.data
            .inplace_parallel_chunk_move(new_size, old_offsets, old_degree, new_offsets);
    }

    fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        self.data
            .inplace_parallel_range_move(new_size, range_diff);
    }
}

pub fn create_column(dt: DataType, path: &str, size: usize) -> Box<dyn Column> {
    match dt {
        DataType::Int32 => Box::new(Int32Column::create(path, size)),
        DataType::Int64 => Box::new(Int64Column::create(path, size)),
        DataType::UInt64 => Box::new(UInt64Column::create(path, size)),
        _ => {
            panic!("not implemented...");
        }
    }
}
