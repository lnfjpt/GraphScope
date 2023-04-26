use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::time::Instant;

use graph_store::prelude::*;
use itertools::__std_iter::Iterator;
use mcsr::graph_db::GlobalCsrTrait;
use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_benchmark::queries;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
    #[structopt(short = "q", long = "query")]
    query_path: String,
    #[structopt(short = "p", long = "print")]
    print_result: bool,
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Result {
    query_type: String,
    query_input: Vec<String>,
    query_result: Vec<Vec<String>>,
}

fn main() {
    let config: Config = Config::from_args();

    pegasus_common::logs::init_log();

    if !crate::queries::graph::DATA_PATH.is_empty() {
        crate::queries::graph::GRAPH.get_current_partition();
    } else {
        crate::queries::graph::CSR.get_current_partition();
    }

    let server_conf = if let Some(ref servers) = config.servers {
        let servers = std::fs::read_to_string(servers).unwrap();
        Configuration::parse(&servers).unwrap()
    } else {
        Configuration::singleton()
    };
    pegasus::startup(server_conf).ok();
    pegasus::wait_servers_ready(&ServerConf::All);

    let query_path = config.query_path;
    let mut queries = vec![];
    let file = File::open(query_path).unwrap();
    let lines = io::BufReader::new(file).lines();
    for line in lines {
        queries.push(line.unwrap());
    }
    let query_start = Instant::now();
    let mut index = 0i32;
    for query in queries {
        let split = query.trim().split("|").collect::<Vec<&str>>();
        let query_name = split[0].clone();
        let mut conf = JobConf::new(query_name.clone().to_owned() + "-" + &index.to_string());
        conf.set_workers(config.workers);
        conf.reset_servers(ServerConf::All);
        // conf.plan_print = true;
        match split[0] {
            "bi2" => {
                println!("Start run query \"BI 2\"");
                let result = queries::bi2(conf, split[1].to_string(), split[2].to_string());
                if config.print_result {
                    let input = vec![split[1].to_string(), split[2].to_string()];
                    let mut result_list = vec![];
                    for x in result {
                        let (tag, count1, count2, diff) = x.unwrap();
                        result_list.push(vec![
                            tag,
                            count1.to_string(),
                            count2.to_string(),
                            diff.to_string(),
                        ])
                    }
                    let query_result = Result {
                        query_type: split[0].to_string(),
                        query_input: input,
                        query_result: result_list,
                    };
                    println!("{:?}", query_result);
                }
            }
            "bi2_gie" => {
                println!("Start run query \"BI GIE 2\"");
                let result = queries::bi2_gie(conf, split[1].to_string(), split[2].to_string());
                if config.print_result {
                    let input = vec![split[1].to_string(), split[2].to_string()];
                    let mut result_list = vec![];
                    for x in result {
                        let (tag, count1, count2, diff) = x.unwrap();
                        result_list.push(vec![
                            tag,
                            count1.to_string(),
                            count2.to_string(),
                            diff.to_string(),
                        ])
                    }
                    let query_result = Result {
                        query_type: split[0].to_string(),
                        query_input: input,
                        query_result: result_list,
                    };
                    println!("{:?}", query_result);
                }
            }
            "vertex_traverse" => {
                println!("Start run query \"Vertex Traverse\"");
                let result = queries::vertex_traverse(conf);
                if config.print_result {
                    let input = vec![];
                    let mut result_list = vec![];
                    for x in result {
                        let final_result = x.unwrap();
                        result_list.push(vec![final_result.to_string()]);
                    }
                    let query_result = Result {
                        query_type: split[0].to_string(),
                        query_input: input,
                        query_result: result_list,
                    };
                    println!("{:?}", query_result);
                }
            }
            "edge_traverse" => {
                println!("Start run query \"Edge Traverse\"");
                let result = queries::edge_traverse(conf);
                if config.print_result {
                    let input = vec![];
                    let mut result_list = vec![];
                    for x in result {
                        let final_result = x.unwrap();
                        result_list.push(vec![final_result.to_string()]);
                    }
                    let query_result = Result {
                        query_type: split[0].to_string(),
                        query_input: input,
                        query_result: result_list,
                    };
                    println!("{:?}", query_result);
                }
            }
            "property_traverse" => {
                println!("Start run query \"Property Traverse\"");
                let result = queries::property_traverse(conf);
                if config.print_result {
                    let input = vec![];
                    let mut result_list = vec![];
                    for x in result {
                        let final_result = x.unwrap();
                        result_list.push(vec![final_result.to_string()]);
                    }
                    let query_result = Result {
                        query_type: split[0].to_string(),
                        query_input: input,
                        query_result: result_list,
                    };
                    println!("{:?}", query_result);
                }
            }
            "gie_traverse" => {
                println!("Start run query \"Property Traverse\"");
                let result = queries::gie_traverse(conf);
                if config.print_result {
                    let input = vec![];
                    let mut result_list = vec![];
                    for x in result {
                        let final_result = x.unwrap();
                        result_list.push(vec![final_result.to_string()]);
                    }
                    let query_result = Result {
                        query_type: split[0].to_string(),
                        query_input: input,
                        query_result: result_list,
                    };
                    println!("{:?}", query_result);
                }
            }
            "handwriting_traverse" => {
                println!("Start run query \"HandWriting Traverse\"");
                let result = queries::handwriting_traverse(conf);
                if config.print_result {
                    let input = vec![];
                    let mut result_list = vec![];
                    for x in result {
                        let final_result = x.unwrap();
                        result_list.push(vec![final_result.to_string()]);
                    }
                    let query_result = Result {
                        query_type: split[0].to_string(),
                        query_input: input,
                        query_result: result_list,
                    };
                    println!("{:?}", query_result);
                }
            }
            "csr_traverse" => {
                println!("Start run query \"HandWriting Traverse\"");
                let result = queries::csr_traverse(conf);
                if config.print_result {
                    let input = vec![];
                    let mut result_list = vec![];
                    for x in result {
                        let final_result = x.unwrap();
                        result_list.push(vec![final_result.to_string()]);
                    }
                    let query_result = Result {
                        query_type: split[0].to_string(),
                        query_input: input,
                        query_result: result_list,
                    };
                    println!("{:?}", query_result);
                }
            }
            "record_traverse" => {
                println!("Start run query \"HandWriting Traverse\"");
                let result = queries::record_traverse(conf);
                if config.print_result {
                    let input = vec![];
                    let mut result_list = vec![];
                    for x in result {
                        let final_result = x.unwrap();
                        result_list.push(vec![final_result.to_string()]);
                    }
                    let query_result = Result {
                        query_type: split[0].to_string(),
                        query_input: input,
                        query_result: result_list,
                    };
                    println!("{:?}", query_result);
                }
            }
            // "bi2_record" => {
            //     println!("Start run query \"BI RECORD 2\"");
            //     let result = queries::bi2_record(conf, split[1].to_string(), split[2].to_string());
            //     if config.print_result {
            //         let input = vec![split[1].to_string(), split[2].to_string()];
            //         let mut result_list = vec![];
            //         for x in result {
            //             let (tag, count1, count2, diff) = x.unwrap();
            //             result_list.push(vec![
            //                 tag,
            //                 count1.to_string(),
            //                 count2.to_string(),
            //                 diff.to_string(),
            //             ])
            //         }
            //         let query_result = Result {
            //             query_type: split[0].to_string(),
            //             query_input: input,
            //             query_result: result_list,
            //         };
            //         println!("{:?}", query_result);
            //     }
            // }

            /*            // as a two-hop case, we compare the versions of bi2_hop, bi2_hop_record, bi2_hop_record_aliasopt, and bi2_hop_record_evalopt, where
            // bi2_hop is the basic handwritten version;
            // bi2_hop_record =  bi2_hop + record;
            // bi2_hop_record_aliasopt = bi2_hop_record + alias_when_necessray_opt;
            // bi2_hop_record_evalopt = bi2_hop_record + simple_prop_eval_opt;
            "bi2_hop" => {
                println!("Start run query \"BI 2 HOP\"");
                let mut result = queries::bi2_hop(conf, split[1].to_string(), split[2].to_string());
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("BI 2 HOP count {:?}", res);
                    }
                }
            }
            "bi2_sub_hop" => {
                println!("Start run query \"BI 2 SUB HOP\"");
                let mut result = queries::bi2_sub_hop(conf, split[1].to_string(), split[2].to_string());
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("BI 2 SUB HOP count {:?}", res);
                    }
                }
            }
            "bi2_hop_record" => {
                println!("Start run query \"BI 2 HOP Record\"");
                let mut result = queries::bi2_hop_record(conf, split[1].to_string(), split[2].to_string());
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("BI 2 HOP record count {:?}", res);
                    }
                }
            }
            "bi2_hop_record_aliasopt" => {
                println!("Start run query \"BI 2 HOP RecordAliasOpt\"");
                let mut result =
                    queries::bi2_hop_record_aliasopt(conf, split[1].to_string(), split[2].to_string());
                if config.print_result {
                    println!("BI 2 HOP record aliasopt count {:?}", result.next());
                }
            }
            "bi2_hop_record_evalopt" => {
                println!("Start run query \"BI 2 HOP EvalOpt\"");
                let mut result =
                    queries::bi2_hop_record_evalopt(conf, split[1].to_string(), split[2].to_string());
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("BI 2 HOP record evalopt count {:?}", res);
                    }
                }
            }
            "bi2_hop_record_filter_pushdown" => {
                println!("Start run query \"BI 2 HOP FilterPD\"");
                let mut result = queries::bi2_hop_record_filter_pushdown(
                    conf,
                    split[1].to_string(),
                    split[2].to_string(),
                );
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("BI 2 HOP record filter pd count {:?}", res);
                    }
                }
            }
            "bi2_hop_record_recordopt" => {
                println!("Start run query \"BI 2 HOP RecordOpt\"");
                let mut result =
                    queries::bi2_hop_record_recordopt(conf, split[1].to_string(), split[2].to_string());
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("BI 2 HOP record record opt count {:?}", res);
                    }
                }
            }

            // khop...
            "khop" => {
                println!("Start run query \"khop\"");
                let mut result = queries::khop(conf);
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("khop {:?}", res);
                    }
                }
            }

            "khop_sub" => {
                println!("Start run query \"khop_sub\"");
                let mut result = queries::khop_sub(conf);
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("khop_sub {:?}", res);
                    }
                }
            }

            "khop_record" => {
                println!("Start run query \"khop_record\"");
                let mut result = queries::khop_record(conf);
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("khop_record {:?}", res);
                    }
                }
            }

            "khop_record_aliasopt" => {
                println!("Start run query \"khop_record_aliasopt\"");
                let mut result = queries::khop_record_aliasopt(conf);
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("khop_record_aliasopt {:?}", res);
                    }
                }
            }

            "khop_record_precache" => {
                println!("Start run query \"khop_record_precache\"");
                let mut result = queries::khop_record_precache(conf);
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("khop_record_precache {:?}", res);
                    }
                }
            }

            "khop_record_recordopt" => {
                println!("Start run query \"khop_record_recordopt\"");
                let mut result = queries::khop_record_recordopt(conf);
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("khop_record_recordopt {:?}", res);
                    }
                }
            }

            "khop_record_recordopt_entry" => {
                println!("Start run query \"khop_record_recordopt_entry\"");
                let mut result = queries::khop_record_recordopt_entry(conf);
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("khop_record_recordopt_entry {:?}", res);
                    }
                }
            }

            "khop_record_aliasrecordopt" => {
                println!("Start run query \"khop_record_aliasrecordopt\"");
                let mut result = queries::khop_record_aliasrecordopt(conf);
                if config.print_result {
                    while let Some(res) = result.next() {
                        println!("khop_record_aliasrecordopt {:?}", res);
                    }
                }
            }*/
            _ => println!("Unknown query"),
        }
        index += 1;
    }
    pegasus::shutdown_all();
    println!("Finished query, elapsed time: {:?}", query_start.elapsed());
}