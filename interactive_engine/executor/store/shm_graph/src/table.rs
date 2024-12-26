use std::collections::HashMap;
use std::usize;

use crate::dataframe::DataFrame;

use crate::columns::*;

pub struct Table {
    columns: Vec<Box<dyn Column>>,
    header: HashMap<String, usize>,
    row_num: usize,
}

impl Table {
    pub fn load(prefix: &str, col_headers: &[(String, DataType)], name: &str) {
        let col_num = col_headers.len();

        for col_i in 0..col_num {
            let col_type = col_headers[col_i].1;
            let col_path = format!("{}_col_{}", prefix, col_i);
            let col_name = format!("{}_col_{}", name, col_i);

            match col_type {
                DataType::Int32 => {
                    Int32Column::load(col_path.as_str(), col_name.as_str());
                }
                DataType::UInt32 => {
                    UInt32Column::load(col_path.as_str(), col_name.as_str());
                }
                DataType::Int64 => {
                    Int64Column::load(col_path.as_str(), col_name.as_str());
                }
                DataType::UInt64 => {
                    UInt64Column::load(col_path.as_str(), col_name.as_str());
                }
                DataType::String => {
                    StringColumn::load(col_path.as_str(), col_name.as_str());
                }
                DataType::LCString => {
                    LCStringColumn::load(col_path.as_str(), col_name.as_str());
                }
                DataType::Double => {
                    DoubleColumn::load(col_path.as_str(), col_name.as_str());
                }
                DataType::Date => {
                    DateColumn::load(col_path.as_str(), col_name.as_str());
                }
                DataType::DateTime => {
                    DateTimeColumn::load(col_path.as_str(), col_name.as_str());
                }
                DataType::ID => {
                    IDColumn::load(col_path.as_str(), col_name.as_str());
                }
                DataType::NULL => {
                    error!("Unexpected column type");
                }
            }
        }
    }

    pub fn open(prefix: &str, col_headers: &[(String, DataType)]) -> Self {
        let col_num = col_headers.len();
        let mut header = HashMap::new();
        let mut columns = Vec::with_capacity(col_num);
        if col_num == 0 {
            return Self { columns, header, row_num: 0 };
        }

        for col_i in 0..col_num {
            let col_name = col_headers[col_i].0.clone();
            let col_type = col_headers[col_i].1;
            header.insert(col_name, col_i);
            let col_path = format!("{}_col_{}", prefix, col_i);

            match col_type {
                DataType::Int32 => {
                    columns.push(Box::new(Int32Column::open(col_path.as_str())));
                }
                DataType::UInt32 => {
                    columns.push(Box::new(UInt32Column::open(col_path.as_str())));
                }
                DataType::Int64 => {
                    columns.push(Box::new(Int64Column::open(col_path.as_str())));
                }
                DataType::UInt64 => {
                    columns.push(Box::new(UInt64Column::open(col_path.as_str())));
                }
                DataType::String => {
                    columns.push(Box::new(StringColumn::open(col_path.as_str())));
                }
                DataType::LCString => {
                    columns.push(Box::new(LCStringColumn::open(col_path.as_str())));
                }
                DataType::Double => {
                    columns.push(Box::new(DoubleColumn::open(col_path.as_str())));
                }
                DataType::Date => {
                    columns.push(Box::new(DateColumn::open(col_path.as_str())));
                }
                DataType::DateTime => {
                    columns.push(Box::new(DateTimeColumn::open(col_path.as_str())));
                }
                DataType::ID => {
                    columns.push(Box::new(IDColumn::open(col_path.as_str())));
                }
                DataType::NULL => {
                    error!("Unexpected column type");
                }
            }
        }

        let mut row_num = usize::MAX;
        for col in columns.iter() {
            row_num = row_num.min(col.len());
        }
        Self { columns, header, row_num }
    }

    pub fn from_column(prop_name: &str, prop_col: Box<dyn Column>) -> Self {
        let mut header = HashMap::new();
        header.insert(prop_name.to_string(), 0);
        let row_num = prop_col.len();
        Self { columns: vec![prop_col], header, row_num }
    }

    pub fn col_num(&self) -> usize {
        self.columns.len()
    }

    pub fn row_num(&self) -> usize {
        self.row_num
    }

    pub fn get_column_by_index(&self, index: usize) -> &'_ Box<dyn Column> {
        &self.columns[index]
    }

    pub fn get_column_by_name(&self, name: &str) -> &'_ Box<dyn Column> {
        let index = self.header.get(name).unwrap();
        &self.columns[*index]
    }

    pub fn get_item(&self, col_name: &str, row_i: usize) -> Option<RefItem> {
        if let Some(col_i) = self.header.get(col_name) {
            self.columns[*col_i].get(row_i)
        } else {
            None
        }
    }

    pub fn get_item_by_index(&self, col_i: usize, row_i: usize) -> Option<RefItem> {
        if col_i < self.columns.len() {
            self.columns[col_i].get(row_i)
        } else {
            None
        }
    }

    pub fn get_row(&self, row_i: usize) -> Option<Vec<Item>> {
        if row_i < self.row_num {
            let mut row = Vec::new();
            for col in self.columns.iter() {
                row.push(col.get(row_i).unwrap().to_owned());
            }
            Some(row)
        } else {
            None
        }
    }

    pub fn set_column(&mut self, col_id: usize, col_name: &str, col: Box<dyn Column>) {
        while self.columns.len() <= col_id {
            self.columns
                .push(Box::new(NullColumn::new(self.row_num)));
        }
        self.columns[col_id] = col;
        self.header.insert(col_name.to_string(), col_id);
    }

    pub fn remove_column(&mut self, name: &str) {
        if let Some(idx) = self.header.get(name) {
            if self.columns.len() > *idx {
                let mut new_columns = vec![];
                for (col_id, col) in self.columns.drain(..).enumerate() {
                    if col_id != *idx {
                        new_columns.push(col);
                    }
                }
                std::mem::replace(&mut self.columns, new_columns);

                // *self.columns = new_columns;

                let mut new_header = HashMap::new();
                for (col_name, col_id) in self.header.iter() {
                    if *col_id < *idx {
                        new_header.insert(col_name.clone(), *col_id);
                    } else if *col_id > *idx {
                        new_header.insert(col_name.clone(), *col_id - 1);
                    }
                }
                // *self.header = new_header;
                std::mem::replace(&mut self.header, new_header);
            }
        }
    }

    pub fn reshuffle_rows(&mut self, indices: &Vec<(usize, usize)>) {
        for col_i in 0..self.col_num() {
            self.columns[col_i].reshuffle(indices);
        }
    }

    pub fn resize(&mut self, new_size: usize) {
        for col_i in 0..self.col_num() {
            self.columns[col_i].resize(new_size)
        }
        self.row_num = new_size;
    }

    pub fn insert_batch(&mut self, offsets: &Vec<usize>, df: &DataFrame) {
        for col in df.columns().iter() {
            let table_col_id = self.header.get(&col.column_name()).unwrap();
            self.columns[*table_col_id].set_column_batch(offsets, col.data())
        }
    }

    // TODO: parallelize
    pub fn parallel_move(&mut self, indices: &Vec<(usize, usize)>) {
        for col_id in 0..self.columns.len() {
            self.columns[col_id].reshuffle(indices);
        }
    }

    pub fn inplace_parallel_chunk_move(
        &mut self, new_size: usize, old_offsets: &[usize], old_degree: &[i32], new_offsets: &[usize],
    ) {
        for col_id in 0..self.columns.len() {
            self.columns[col_id].inplace_parallel_chunk_move(
                new_size,
                old_offsets,
                old_degree,
                new_offsets,
            );
        }
    }

    pub fn inplace_parallel_range_move(&mut self, new_size: usize, range_diff: &[(usize, usize, i64)]) {
        for col_id in 0..self.columns.len() {
            self.columns[col_id].inplace_parallel_range_move(
                new_size,
                range_diff,
            );
        }

    }
}

unsafe impl Sync for Table {}
unsafe impl Send for Table {}
