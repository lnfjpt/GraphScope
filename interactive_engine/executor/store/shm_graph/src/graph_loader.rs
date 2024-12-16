use std::path::{Path, PathBuf};
use std::fs::{create_dir_all, read_dir, File};
use regex::Regex;

use crate::error::{GDBError, GDBResult};

pub fn get_files_list(prefix: &PathBuf, file_strings: &Vec<String>) -> GDBResult<Vec<PathBuf>> {
    let mut path_lists = vec![];
    for file_string in file_strings {
        let temp_path = PathBuf::from(prefix.to_string_lossy().to_string() + "/" + file_string);
        let filename = temp_path
            .file_name()
            .ok_or(GDBError::UnknownError)?
            .to_str()
            .ok_or(GDBError::UnknownError)?;
        if filename.contains("*") {
            let re_string = "^".to_owned() + &filename.replace(".", "\\.").replace("*", ".*") + "$";
            let re = Regex::new(&re_string).unwrap();
            let parent_dir = temp_path.parent().unwrap();
            for _entry in read_dir(parent_dir)? {
                let entry = _entry?;
                let path = entry.path();
                let fname = path
                    .file_name()
                    .ok_or(GDBError::UnknownError)?
                    .to_str()
                    .ok_or(GDBError::UnknownError)?;
                if re.is_match(fname) {
                    path_lists.push(path);
                }
            }
        } else {
            path_lists.push(temp_path);
        }
    }
    Ok(path_lists)
}