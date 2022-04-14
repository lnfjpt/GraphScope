use graph_store::prelude::*;
use pegasus::{Configuration, JobConf};
use pegasus_benchmark::queries;
use std::fs::File;
use std::io::{self, BufRead};
use std::time::Instant;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
    #[structopt(short = "q", long = "query")]
    query_path: String,
    #[structopt(short = "p", long = "print")]
    print_result: bool,
}

fn main() {
    let config: Config = Config::from_args();

    crate::queries::ldbc::graph::GRAPH.get_current_partition();

    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();

    let query_path = config.query_path;
    let mut queries = vec![];
    let file = File::open(query_path).unwrap();
    let lines = io::BufReader::new(file).lines();
    for line in lines {
        queries.push(line.unwrap());
    }
    let query_start = Instant::now();
    let mut index = 0;
    for query in queries {
        let split = query.trim().split(",").collect::<Vec<&str>>();
        let query_name = split[0].clone();
        let mut conf = JobConf::new(query_name.clone().to_owned() + "-" + &index.to_string());
        conf.set_workers(config.workers);
        match split[0] {
            "is1" => {
                println!("Start run query \"Interactive Short 1\"");
                queries::is1(conf, split[1].parse::<u64>().unwrap());
                ()
            }
            "is3" => {
                println!("Start run query \"Interactive Short 3\"");
                queries::is3(conf, split[1].parse::<u64>().unwrap());
                ()
            }
            "is4" => {
                println!("Start run query \"Interactive Short 4\"");
                queries::is4(conf, split[1].parse::<u64>().unwrap());
                ()
            }
            "is5" => {
                println!("Start run query \"Interactive Short 5\"");
                queries::is5(conf, split[1].parse::<u64>().unwrap());
                ()
            }
            "is6" => {
                println!("Start run query \"Interactive Short 6\"");
                queries::is6(conf, split[1].parse::<u64>().unwrap());
                ()
            }
            "is7" => {
                println!("Start run query \"Interactive Short 7\"");
                queries::is7(conf, split[1].parse::<u64>().unwrap());
                ()
            }
            "ic1" => {
                println!("Start run query \"Interactive Complex 1\"");
                let result = queries::ic1_ir(conf, split[1].parse::<u64>().unwrap(), split[2].to_string());
                if config.print_result {
                    for x in result {
                        let (id, last_name, distance, birthday, creation_date, gender, browser, ip) =
                            x.unwrap();
                        println!(
                            "{} {} {} {} {} {} {} {}",
                            id, last_name, distance, birthday, creation_date, gender, browser, ip
                        );
                    }
                }
                ()
            }
            "ic2" => {
                println!("Start run query \"Interactive Complex 2\"");
                let result = queries::ic2(conf, split[1].parse::<u64>().unwrap(), split[2].to_string());
                if config.print_result {
                    for x in result {
                        let (friend_id, first_name, last_name, message_id, content, create_date) =
                            x.unwrap();
                        println!(
                            "{} {} {} {} {} {}",
                            friend_id, first_name, last_name, message_id, content, create_date
                        );
                    }
                }
                ()
            }
            "ic3" => {
                println!("Start run query \"Interactive Complex 3\"");
                let result = queries::ic3(
                    conf,
                    split[1].parse::<u64>().unwrap(),
                    split[4].to_string(),
                    split[5].to_string(),
                    split[2].to_string(),
                    split[3].parse::<i32>().unwrap(),
                );
                if config.print_result {
                    for x in result {
                        let (person_id, first_name, last_name, x_count, y_count, count) = x.unwrap();
                        println!(
                            "{} {} {} {} {} {}",
                            person_id, first_name, last_name, x_count, y_count, count
                        );
                    }
                }
                ()
            }
            "ic4" => {
                println!("Start run query \"Interactive Complex 4\"");
                let result = queries::ic4(
                    conf,
                    split[1].parse::<u64>().unwrap(),
                    split[2].to_string(),
                    split[3].parse::<i32>().unwrap(),
                );
                if config.print_result {
                    for x in result {
                        let (tag_name, count) = x.unwrap();
                        println!("{} {} ", tag_name, count);
                    }
                }
                ()
            }
            "ic5" => {
                println!("Start run query \"Interactive Complex 5\"");
                let result = queries::ic5(conf, split[1].parse::<u64>().unwrap(), split[2].to_string());
                if config.print_result {
                    for x in result {
                        let (forum_id, count) = x.unwrap();
                        println!("{} {} ", forum_id, count);
                    }
                }
                ()
            }
            "ic6" => {
                println!("Start run query \"Interactive Complex 6\"");
                let result = queries::ic6(conf, split[1].parse::<u64>().unwrap(), split[2].to_string());
                if config.print_result {
                    for x in result {
                        let (tag_name, count) = x.unwrap();
                        println!("{} {} ", tag_name, count);
                    }
                }
                ()
            }
            "ic6_nosub" => {
                println!("Start run query \"Interactive Complex 6 NoSub\"");
                let result =
                    queries::ic6_nosub(conf, split[1].parse::<u64>().unwrap(), split[2].to_string());
                if config.print_result {
                    for x in result {
                        let (tag_name, count) = x.unwrap();
                        println!("{} {} ", tag_name, count);
                    }
                }
                ()
            }
            "ic7" => {
                println!("Start run query \"Interactive Complex 7\"");
                let result = queries::ic7(conf, split[1].parse::<u64>().unwrap());
                if config.print_result {
                    for x in result {
                        let (
                            friend_id,
                            first_name,
                            last_name,
                            like_date,
                            message_id,
                            content,
                            minutes,
                            is_new,
                        ) = x.unwrap();
                        println!(
                            "{} {} {} {} {} {} {} {}",
                            friend_id,
                            first_name,
                            last_name,
                            like_date,
                            message_id,
                            content,
                            minutes,
                            is_new
                        );
                    }
                }
                ()
            }
            "ic8" => {
                println!("Start run query \"Interactive Complex 8\"");
                let result = queries::ic8(conf, split[1].parse::<u64>().unwrap());
                if config.print_result {
                    for x in result {
                        let (friend_id, first_name, last_name, comment_date, comment_id, content) =
                            x.unwrap();
                        println!(
                            "{} {} {} {} {} {}",
                            friend_id, first_name, last_name, comment_date, comment_id, content
                        );
                    }
                }
                ()
            }
            "ic9" => {
                println!("Start run query \"Interactive Complex 9\"");
                let result = queries::ic9(conf, split[1].parse::<u64>().unwrap());
                if config.print_result {
                    for x in result {
                        let (friend_id, first_name, last_name, message_id, content, message_date) =
                            x.unwrap();
                        println!(
                            "{} {} {} {} {} {}",
                            friend_id, first_name, last_name, message_id, content, message_date
                        );
                    }
                }
                ()
            }
            "ic10" => {
                println!("Start run query \"Interactive Complex 10\"");
                let result =
                    queries::ic10(conf, split[1].parse::<u64>().unwrap(), split[2].parse::<i32>().unwrap());
                if config.print_result {
                    for x in result {
                        let (friend_id, first_name, last_name, score, gender, city) = x.unwrap();
                        println!(
                            "{} {} {} {} {} {}",
                            friend_id, first_name, last_name, score, gender, city
                        );
                    }
                }
                ()
            }
            "ic11" => {
                println!("Start run query \"Interactive Complex 11\"");
                let result = queries::ic11(
                    conf,
                    split[1].parse::<u64>().unwrap(),
                    split[2].to_string(),
                    split[3].parse::<i32>().unwrap(),
                );
                if config.print_result {
                    for x in result {
                        let (friend_id, first_name, last_name, org_name, works_from) = x.unwrap();
                        println!("{} {} {} {} {}", friend_id, first_name, last_name, org_name, works_from);
                    }
                }
                ()
            }
            "ic12" => {
                println!("Start run query \"Interactive Complex 12\"");
                let result = queries::ic12(conf, split[1].parse::<u64>().unwrap(), split[2].to_string());
                if config.print_result {
                    for x in result {
                        let (friend_id, first_name, last_name, tag_list, count) = x.unwrap();
                        println!("{} {} {} {:?} {}", friend_id, first_name, last_name, tag_list, count);
                    }
                }
                ()
            }
            "ic12_nosub" => {
                println!("Start run query \"Interactive Complex 12 NoSub\"");
                let result =
                    queries::ic12_nosub(conf, split[1].parse::<u64>().unwrap(), split[2].to_string());
                if config.print_result {
                    for x in result {
                        let (friend_id, first_name, last_name, count) = x.unwrap();
                        println!("{} {} {} {}", friend_id, first_name, last_name, count);
                    }
                }
                ()
            }
            "ic13" => {
                println!("Start run query \"Interactive Complex 13\"");
                let result =
                    queries::ic13(conf, split[1].parse::<u64>().unwrap(), split[2].parse::<u64>().unwrap());
                if config.print_result {
                    let mut printed = false;
                    for x in result {
                        let step = x.unwrap();
                        println!("{}", step);
                        printed = true;
                    }
                    if !printed {
                        println!("-1");
                    }
                }
                ()
            }
            "ic14" => {
                println!("Start run query \"Interactive Complex 14\"");
                let result =
                    queries::ic14(conf, split[1].parse::<u64>().unwrap(), split[2].parse::<u64>().unwrap());
                if config.print_result {
                    for x in result {
                        let (path, weight) = x.unwrap();
                        println!("{:?} {}", path, weight);
                    }
                }
                ()
            }
            "bi1" => {
                println!("Start run query \"BI 1\"");
                let result = queries::bi1(conf, split[1].to_string());
                if config.print_result {
                    for x in result {
                        let (year, is_comment, length, count, avg_length, sum_length) = x.unwrap();
                        println!("{} {} {} {} {} {}", year, is_comment, length, count, avg_length, sum_length);
                    }
                }
                ()
            }
            "bi2" => {
                println!("Start run query \"BI 2\"");
                let result = queries::bi2(
                    conf,
                    split[1].to_string(),
                    split[2].to_string(),
                    split[3].to_string(),
                    split[4].to_string(),
                );
                if config.print_result {
                    for x in result {
                        let (country, month, gender, age, tag, count) = x.unwrap();
                        println!("{} {} {} {} {} {}", country, month, gender, age, tag, count);
                    }
                }
                ()
            }
            "bi3" => {
                println!("Start run query \"BI 3\"");
                let result =
                    queries::bi3(conf, split[1].parse::<i32>().unwrap(), split[2].parse::<i32>().unwrap());
                if config.print_result {
                    for x in result {
                        let (tag, count1, count2, diff) = x.unwrap();
                        println!("{} {} {} {}", tag, count1, count2, diff);
                    }
                }
                ()
            }
            _ => println!("Unknown query"),
        }
        index += 1;
    }
    pegasus::shutdown_all();
    println!("Finished query, elapsed time: {:?}", query_start.elapsed());
}
