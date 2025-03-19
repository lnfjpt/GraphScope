use std::fmt::format;
use std::io::{Write, BufWriter};
use std::any::Any;
use std::collections::HashMap;
use std::fs::File;

use csv::StringRecord;
use serde::ser::{Serialize, Serializer, SerializeSeq};
use serde::de::{self, Deserialize, Deserializer, Visitor};

use crate::columns::DataType;
use crate::dataframe::*;
use crate::date::Date;
use crate::date_time::DateTime;
use crate::types::DefaultId;

pub struct RecordBatch {
    pub columns: Vec<Box<dyn HeapColumn>>,
}

impl RecordBatch {
    pub fn new(cols: &[DataType]) -> Self {
        let mut columns: Vec<Box<dyn HeapColumn>> = vec![];
        for col in cols.iter() {
            match *col {
                DataType::Date => {
                    columns.push(Box::new(DateHColumn::new()));
                }
                DataType::DateTime => {
                    columns.push(Box::new(DateTimeHColumn::new()));
                }
                DataType::Int32 => {
                    columns.push(Box::new(I32HColumn::new()));
                }
                DataType::String => {
                    columns.push(Box::new(StringHColumn::new()));
                }
                DataType::LCString => {
                    columns.push(Box::new(StringHColumn::new()));
                }
                DataType::ID => {
                    columns.push(Box::new(IDHColumn::new()));
                }
                _ => {
                    panic!("RecordBatch::new type - {:?} not support", *col);
                }
            }
        }
        Self { columns }
    }

    pub fn append(&mut self, record: &StringRecord, picked_cols: &[usize]) {
        for (idx, col) in picked_cols.iter().enumerate() {
            self.columns[idx].append_str(record.get(*col).unwrap());
        }
    }

    pub fn resize(&mut self, new_len: usize) {
        for i in 0..self.columns.len() {
            self.columns[i].resize(new_len);
        }
    }

    pub fn set_indexed_rb(&mut self, idx: &[usize], other: &RecordBatch) {
        let row_num = idx.len();
        for i in 0..self.columns.len() {
            for j in 0..row_num {
                if idx[j] != usize::MAX {
                    self.columns[i].set_ref(idx[j], other.columns[i].get(j).unwrap());
                }
            }
        }
    }

    pub fn col_types(&self) -> Vec<DataType> {
        let mut col_types = vec![];
        for col in self.columns.iter() {
            col_types.push(col.get_type());
        }
        col_types
    }

    pub fn append_rb(&mut self, other: RecordBatch) {
        assert_eq!(self.columns.len(), other.columns.len());
        for (i, col) in other.columns.into_iter().enumerate() {
            self.columns[i].append_col(col);
        }
    }
}

unsafe impl Send for RecordBatch {}

unsafe impl Sync for RecordBatch {}

impl Serialize for RecordBatch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
        let mut col_types = vec![];
        for col in self.columns.iter() {
            col_types.push(col.get_type());
        }

        let mut seq = serializer.serialize_seq(Some(self.columns.len() + 1))?;
        seq.serialize_element(&col_types)?;

        for col in self.columns.iter() {
            let dt = col.get_type();
            match dt {
                DataType::Int32 => {
                    let buf = &col
                        .as_any()
                        .downcast_ref::<I32HColumn>()
                        .unwrap()
                        .data;
                    seq.serialize_element(buf)?;
                }
                DataType::Date => {
                    let buf = &col
                        .as_any()
                        .downcast_ref::<DateHColumn>()
                        .unwrap()
                        .data;
                    seq.serialize_element(buf)?;
                }
                DataType::DateTime => {
                    let buf = &col
                        .as_any()
                        .downcast_ref::<DateTimeHColumn>()
                        .unwrap()
                        .data;
                    seq.serialize_element(buf)?;
                }
                DataType::String => {
                    let buf = &col
                        .as_any()
                        .downcast_ref::<StringHColumn>()
                        .unwrap()
                        .data;
                    seq.serialize_element(buf)?;
                }
                DataType::ID => {
                    let buf = &col
                        .as_any()
                        .downcast_ref::<IDHColumn>()
                        .unwrap()
                        .data;
                    seq.serialize_element(buf)?;
                }
                _ => {
                    panic!("RecordBatch::serialize type - {:?} not support", dt);
                }
            }
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for RecordBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de> {
        struct RecordBatchVisitor;
        impl<'de> Visitor<'de> for RecordBatchVisitor {
            type Value = RecordBatch;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a seq of Vec<DataType> and HeapColumns")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::SeqAccess<'de>, {
                let col_types: Vec<DataType> = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let mut columns: Vec<Box<dyn HeapColumn>> = vec![];
                for (col_idx, col_type) in col_types.into_iter().enumerate() {
                    match col_type {
                        DataType::Date => {
                            let col: Vec<Date> = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(col_idx + 1, &self))?;
                            columns.push(Box::new(DateHColumn::from(col)));
                        }
                        DataType::DateTime => {
                            let col: Vec<DateTime> = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(col_idx + 1, &self))?;
                            columns.push(Box::new(DateTimeHColumn::from(col)));
                        }
                        DataType::Int32 => {
                            let col: Vec<i32> = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(col_idx + 1, &self))?;
                            columns.push(Box::new(I32HColumn::from(col)));
                        }
                        DataType::Int64 => {
                            let col: Vec<i64> = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(col_idx + 1, &self))?;
                            columns.push(Box::new(I64HColumn::from(col)));
                        }
                        DataType::String => {
                            let col: Vec<String> = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(col_idx + 1, &self))?;
                            columns.push(Box::new(StringHColumn::from(col)));
                        }
                        DataType::ID => {
                            let col: Vec<DefaultId> = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(col_idx + 1, &self))?;
                            columns.push(Box::new(IDHColumn::from(col)));
                        }
                        _ => {
                            panic!("RecordBatch::deserialize type - {:?} not support", col_type);
                        }
                    }
                }
                Ok(RecordBatch { columns })
            }
        }

        deserializer.deserialize_seq(RecordBatchVisitor)
    }
}

pub trait HeapColumnWriter {
    fn write(&mut self, col: &Box<dyn HeapColumn>);
    fn flush(&mut self);
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

pub struct PodColumnWriter {
    writer: BufWriter<File>,
}

impl PodColumnWriter {
    pub fn new(path: &str) -> Self {
        let file = File::create(path).unwrap();
        let writer = BufWriter::new(file);
        Self { writer }
    }

    pub fn append_i32(&mut self, val: i32) {
        self.writer.write_all(&val.to_le_bytes()).unwrap();
    }

    pub fn append_datetime(&mut self, val: DateTime) {
        self.writer.write_all(&val.to_i64().to_le_bytes()).unwrap();
    }

    pub fn append_date(&mut self, val: Date) {
        self.writer.write_all(&val.to_i32().to_le_bytes()).unwrap();
    }

    pub fn append_u16(&mut self, val: u16) {
        self.writer.write_all(&val.to_le_bytes()).unwrap();
    }

    pub fn append_usize(&mut self, val: usize) {
        self.writer.write_all(&val.to_le_bytes()).unwrap();
    }

    pub fn append_bytes(&mut self, val: &[u8]) {
        self.writer.write_all(val).unwrap();
    }
}

impl HeapColumnWriter for PodColumnWriter {
    fn write(&mut self, col: &Box<dyn HeapColumn>) {
        let dt = col.get_type();
        match dt {
            DataType::Int32 => {
                let buf = &col
                    .as_any()
                    .downcast_ref::<I32HColumn>()
                    .unwrap()
                    .data;
                for v in buf.iter() {
                    self.append_i32(*v);
                }
            }
            DataType::Date => {
                let buf = &col
                    .as_any()
                    .downcast_ref::<DateHColumn>()
                    .unwrap()
                    .data;
                for v in buf.iter() {
                    self.append_date(*v);
                }
            }
            DataType::DateTime => {
                let buf = &col
                    .as_any()
                    .downcast_ref::<DateTimeHColumn>()
                    .unwrap()
                    .data;
                for v in buf.iter() {
                    self.append_datetime(*v);
                }
            }
            DataType::ID => {
                let buf = &col
                    .as_any()
                    .downcast_ref::<IDHColumn>()
                    .unwrap()
                    .data;
                for v in buf.iter() {
                    self.append_usize(*v);
                }
            }
            _ => {
                panic!("PodColumnWriter::write type - {:?} not support", dt);
            }
        }
    }

    fn flush(&mut self) {
        self.writer.flush().unwrap()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct StringColumnWriter {
    offset_writer: PodColumnWriter,
    length_writer: PodColumnWriter,
    content_writer: PodColumnWriter,

    cur_offset: usize,
}

impl StringColumnWriter {
    pub fn new(path: &str) -> Self {
        Self {
            offset_writer: PodColumnWriter::new(format!("{}_offset", path).as_str()),
            length_writer: PodColumnWriter::new(format!("{}_length", path).as_str()),
            content_writer: PodColumnWriter::new(format!("{}_content", path).as_str()),
            cur_offset: 0_usize,
        }
    }

    pub fn append_impl(&mut self, val: &String) {
        self.offset_writer.append_usize(self.cur_offset);
        self.cur_offset += val.len();
        self.length_writer.append_u16(val.len() as u16);
        self.content_writer.append_bytes(val.as_bytes());
    }
}

impl HeapColumnWriter for StringColumnWriter {
    fn write(&mut self, col: &Box<dyn HeapColumn>) {
        let dt = col.get_type();
        match dt {
            DataType::String => {
                let buf = &col
                    .as_any()
                    .downcast_ref::<StringHColumn>()
                    .unwrap()
                    .data;
                for v in buf.iter() {
                    self.append_impl(v);
                }
            }
            _ => {
                info!("StringColumnWriter::write type - {:?} not support...", dt);
            }
        }
    }
    fn flush(&mut self) {
        self.offset_writer.flush();
        self.length_writer.flush();
        self.content_writer.flush();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct LCStringColumnWriter {
    table: HashMap<String, u16>,

    index_writer: PodColumnWriter,
    data_writer: StringColumnWriter,
}

impl LCStringColumnWriter {
    pub fn new(path: &str) -> Self {
        Self {
            table: HashMap::<String, u16>::new(),
            index_writer: PodColumnWriter::new(format!("{}_index", path).as_str()),
            data_writer: StringColumnWriter::new(format!("{}_data", path).as_str()),
        }
    }

    pub fn append_impl(&mut self, val: &String) {
        if let Some(idx) = self.table.get(val) {
            self.index_writer.append_u16(*idx);
        } else {
            let idx = self.table.len() as u16;
            self.table.insert(val.clone(), idx);
            self.index_writer.append_u16(idx);
        }
    }
}

impl HeapColumnWriter for LCStringColumnWriter {
    fn write(&mut self, col: &Box<dyn HeapColumn>) {
        let dt = col.get_type();
        match dt {
            DataType::String => {
                let buf = &col
                    .as_any()
                    .downcast_ref::<StringHColumn>()
                    .unwrap()
                    .data;
                for v in buf.iter() {
                    self.append_impl(v);
                }
            }
            _ => {
                info!("LCStringColumnWriter::write type - {:?} not support...", dt);
            }
        }
    }

    fn flush(&mut self) {
        self.index_writer.flush();
        let mut string_vec = vec!["".to_string(); self.table.len()];
        for (val, idx) in self.table.iter() {
            string_vec[*idx as usize] = val.clone();
        }
        for val in string_vec.into_iter() {
            self.data_writer.append_impl(&val);
        }
        self.data_writer.flush();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct RecordBatchWriter {
    writers: Vec<Box<dyn HeapColumnWriter>>,
}

impl RecordBatchWriter {
    pub fn new(prefix: &str, cols: &[DataType]) -> Self {
        let mut writers: Vec<Box<dyn HeapColumnWriter>> = vec![];
        for (idx, col) in cols.iter().enumerate() {
            let col_prefix = format!("{}_col_{}", prefix, idx);
            match *col {
                DataType::Date => {
                    writers.push(Box::new(PodColumnWriter::new(&col_prefix)));
                }
                DataType::DateTime => {
                    writers.push(Box::new(PodColumnWriter::new(&col_prefix)));
                }
                DataType::Int32 => {
                    writers.push(Box::new(PodColumnWriter::new(&col_prefix)));
                }
                DataType::String => {
                    writers.push(Box::new(StringColumnWriter::new(&col_prefix)));
                }
                DataType::LCString => {
                    writers.push(Box::new(LCStringColumnWriter::new(&col_prefix)));
                }
                DataType::ID => {
                    writers.push(Box::new(PodColumnWriter::new(&col_prefix)));
                }
                _ => {
                    panic!("RecordBatchWriter::new type - {:?} not support...", *col);
                }
            }
        }
        Self { writers }
    }

    pub fn write(&mut self, rb: &RecordBatch) {
        for (idx, writer) in self.writers.iter_mut().enumerate() {
            if let Some(col) = rb.columns.get(idx) {
                writer.write(col);
            }
        }
    }

    pub fn flush(&mut self) {
        for writer in self.writers.iter_mut() {
            writer.flush();
        }
    }
}