use std::collections::HashMap;
use std::usize;

use crate::columns::*;
use crate::vector::*;

pub struct Table {
    columns: Vec<Box<dyn Column>>,
    header: HashMap<String, usize>,
    row_num: usize,
}

impl Table {
    pub fn open(prefix: &str) -> Self {
        let col_names = SharedStringVec::open(format!("{}_col_names", prefix).as_str());
        let col_types = SharedVec::<DataType>::open(format!("{}_col_types", prefix).as_str());

        let col_num = col_names.len().min(col_types.len());
        let mut header = HashMap::new();
        let mut columns = Vec::with_capacity(col_num);
        if col_num == 0 {
            return Self { columns, header, row_num: 0 };
        }

        for col_i in 0..col_num {
            let col_name = col_names.get_unchecked(col_i).to_string();
            let col_type = col_types.get_unchecked(col_i);
            header.insert(col_name.clone(), col_i);
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
        Self {
            columns: vec![prop_col],
            header,
            row_num,
        }
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
            self.columns.push(Box::new(NullColumn::new(self.row_num)));
        }
        self.columns[col_id] = col;
        self.header.insert(col_name.to_string(), col_id);
    }
}

unsafe impl Sync for Table {}
unsafe impl Send for Table {}
