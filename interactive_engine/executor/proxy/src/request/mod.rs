use std::collections::HashMap;
use std::env;
use std::io;

use byteorder::{LittleEndian, ReadBytesExt};
use lazy_static::lazy_static;
use pb::job_service_client::JobServiceClient;
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Response, Streaming};


use crate::generated::common;
use crate::generated::procedure;
use crate::generated::protocol as pb;

lazy_static! {
    static ref ENDPOINT: String = env::var("ENDPOINT").unwrap_or_else(|_| "".to_string());
}
pub struct JobClient {
    client: JobServiceClient<Channel>,
    workers: u32,
}

impl JobClient {
    pub async fn new(endpoint: String, workers: u32) -> Result<JobClient, Box<dyn std::error::Error>> {
        let channel = Channel::from_shared(endpoint)
            .unwrap()
            .connect() // 连接到服务端
            .await?;
        let client = JobServiceClient::new(channel);
        Ok(JobClient { client, workers })
    }

    pub async fn submitProcedure(
        &mut self, job_id: u64, query_name: String, arguments: HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut index = 0;
        let mut inputs = vec![];
        for (param_name, value) in arguments {
            let argument = procedure::Argument {
                param_name,
                param_ind: index,
                value: Some(common::Value { item: Some(common::value::Item::Str(value)) }),
            };
            inputs.push(argument);
            index += 1;
        }
        let query = procedure::Query {
            query_name: Some(common::NameOrId {
                item: Some(common::name_or_id::Item::Name(query_name.clone())),
            }),
            arguments: inputs,
        };
        let mut plan = Vec::new();
        query
            .encode(&mut plan)
            .expect("Failed to encode query");

        let job_config = pb::JobConfig {
            job_id,
            job_name: query_name,
            workers: self.workers,
            time_limit: 0,
            batch_size: 0,
            batch_capacity: 0,
            memory_limit: 0,
            trace_enable: false,
            servers: None,
        };
        let job_request = pb::JobRequest { conf: Some(job_config), source: vec![], plan, resource: vec![] };

        if let Ok(response) = self.client.submit(job_request).await {
            let mut stream = response.into_inner();
            while let Some(message) = stream.next().await {
                match message {
                    Ok(job_response) => {
                        let job_id: u64 = job_response.job_id;
                        let resp_bytes: Vec<u8> = job_response.resp;
                        let mut reader = io::Cursor::new(resp_bytes);
                        let mut buf = [0u8; 8]; // 用于存储u64长度的缓冲区
                        while reader.read_exact(&mut buf).await.is_ok() {
                            let mut cursor = io::Cursor::new(buf);
                            let length = ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
                            let mut buffer = vec![0; length as usize];
                            // 读取指定长度的字节
                            if reader.read_exact(&mut buffer).await.is_err() {
                                break;
                            }
                            let result = match String::from_utf8(buffer) {
                                Ok(s) => s,
                                Err(_) => {
                                    panic!("Invalid result")
                                }
                            };
                            println!("{}\n", result);
                        }
                    }
                    Err(e) => {
                        // 发生错误
                        eprintln!("Stream error: {}", e);
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
