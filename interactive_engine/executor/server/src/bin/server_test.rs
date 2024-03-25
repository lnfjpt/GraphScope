use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use regex::Regex;
use serde::Deserialize;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "q", long = "query", default_value = "")]
    query: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = Config::from_args();
    let query = config.query;
    println!("{}", query);
    let text = "a[['n', 'NODE'],  ['1', '54fg'] ,  ['1', '54fg']],";

    let re = Regex::new(r"\[(?:'[^']*',\s*)+'[^']*'\]").unwrap();

    for cap in re.find_iter(text) {
        println!("Found match: {}", cap.as_str());
    }
    let text = r#"CALL gs.flex.custom.asProcedure(    'bi6_precompute',    'MATCH         (person1:PERSON)<-[:HASCREATOR]-(message:COMMENT|POST)<-[:LIKES]-(person2:PERSON)     RETURN person1 AS id, count(person2) AS popularityScore',    'read',    [['id','Int64'], ['popularityScore', 'Int32']],    [],    'The implementation of BI-6-Precompute')"#;
    // let re = Regex::new(
    //     r"CALL (\w+\.\w+\.\w+)\(\s*'([^']*)',\s*'([^']*)',\s*'([^']*)',\s*(\[\[[^\]]*\]\]),\s*(\[\[[^\]]*\]\]),\s*'([^']*)'"
    // ).unwrap();
    let re = Regex::new(r"^CALL ([^\(]*)\(([\s\S]*?)\)$").unwrap();

    if re.is_match(text) {
        let cap = re.captures(text).unwrap();
        println!("Procedure: {}", &cap[1]);
        println!("parameters: {}", &cap[2]);
        let parameter = &cap[2];
        let re = Regex::new(r"^\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*\[((?:\['[^']*'(?:,\s*'[^']*')*\](?:,\s*)?)*)\]\s*,\s*(\[(?:\['[^']*'(?:,\s*'[^']*')*\](?:,\s*)?)*\])\s*,\s*'([^']*)'\s*$").unwrap();
        if re.is_match(parameter) {
            let cap = re.captures(&cap[2]).unwrap();
            println!("first parameter: {}", &cap[1]);
            println!("second parameter: {}", &cap[2]);
            println!("third parameter: {}", &cap[3]);
            println!("forth parameter: {}", &cap[4]);
            println!("fifth parameter: {}", &cap[5]);
            println!("sixth parameter: {}", &cap[6]);
        }
    }

    let parameter_re = Regex::new(r#""([^"]*)"(?:,|$)"#).unwrap();
    let text = "\"a\", \"b\"";
    for caps in parameter_re.captures_iter(&text) {
        // 第一个捕获组包含参数的内容
        if let Some(matched) = caps.get(1) {
            println!("Matched parameter: {}", matched.as_str());
        }
    }

    let input_text = " ['N', 'node'], ['fa', 'gw']";
    let input_re = Regex::new(r"\['([^']*)',\s*'([^']*)'\]").unwrap();
    for cap in input_re.captures_iter(input_text) {
        let part = format!("('{}', '{}') ", &cap[1], &cap[2]);
        println!("{}", part);
    }
    Ok(())
}
