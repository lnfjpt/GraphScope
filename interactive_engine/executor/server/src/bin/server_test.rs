use std::path::PathBuf;
use std::process::Command;
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
    let gie_dir_str = "/Users/nengli/shirly";
    let temp_dir = "/tmp";
    let query_name = "bi6";
    let cypher_path = format!("{}/{}.cypher", temp_dir, query_name);
    let plan_path = format!("{}/{}.plan", temp_dir, query_name);
    let config_path = format!("{}/{}.yaml", temp_dir, query_name);
    println!("java -cp {}/GraphScope/interactive_engine/compiler/target/compiler-0.0.1-SNAPSHOT-shade.jar \
                      -Djna.library.path={}/GraphScope/interactive_engine/executor/ir/target/release/ \
                      com.alibaba.graphscope.common.ir.tools.GraphPlanner {}/GraphScope/interactive_engine/compiler/conf/ir.compiler.properties \
                      {} {} {} \"name:{}\"", gie_dir_str, gie_dir_str, gie_dir_str, cypher_path, plan_path, config_path, query_name);
    let mut compiler_command = Command::new("java")
        .arg("-cp")
        .arg(format!(
            "{}/GraphScope/interactive_engine/compiler/target/compiler-0.0.1-SNAPSHOT-shade.jar",
            gie_dir_str
        ))
        .arg(
            "-Djna.library.path=".to_owned()
                + gie_dir_str
                + "/GraphScope/interactive_engine/executor/ir/target/release/",
        )
        .arg("com.alibaba.graphscope.common.ir.tools.GraphPlanner")
        .arg(format!("{}/GraphScope/interactive_engine/compiler/conf/ir.compiler.properties", gie_dir_str))
        .arg(cypher_path)
        .arg(plan_path)
        .arg(config_path)
        .arg(format!("name:{}", query_name))
        .status();
    match compiler_command {
        Ok(status) => {
            println!("Finished generate plan");
        }
        Err(e) => {
            // 处理运行命令的错误
            eprintln!("Error executing command: {}", e);
        }
    }
    let codegen_command = "./gen_pegasus_from_plan -i ../benchmark/bi18.plan -n bi18 -t plan -s ../GraphScope/interactive_engine/executor/store/mcsr/schema.json -r single_machine";
    Ok(())
}
