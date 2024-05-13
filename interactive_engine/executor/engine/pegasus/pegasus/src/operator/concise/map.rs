//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.
use crate::api::function::FnResult;
use crate::api::Map;
use crate::api::Unary;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;
use std::time::Instant;

/// A private function of getting a `Map` operator's name as a base name given by the system
/// plus an extra name given by the user
fn _get_name(base: &str, extra: &str) -> String {
    if extra.is_empty() {
        base.to_string()
    } else {
        format!("{}_{}", base, extra)
    }
}

impl<I: Data> Map<I> for Stream<I> {
    fn map_with_name<O, F>(self, name: &str, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        F: Fn(I) -> FnResult<O> + Send + 'static,
    {
        self.unary(&_get_name("map", name), |_info| {
            move |input, output| {
                input.for_each_batch(|batch| {
                    if !batch.is_empty() {
                        let mut session = output.new_session(&batch.tag)?;
                        for item in batch.drain() {
                            let r = func(item)?;
                            session.give(r)?;
                        }
                    }
                    Ok(())
                })
            }
        })
    }

    fn filter_map_with_name<O, F>(self, name: &str, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        F: Fn(I) -> FnResult<Option<O>> + Send + 'static,
    {
        self.unary(&_get_name("filter_map", name), |_info| {
            move |input, output| {
                input.for_each_batch(|batch| {
                    if !batch.is_empty() {
                        let mut session = output.new_session(&batch.tag)?;
                        for item in batch.drain() {
                            if let Some(r) = func(item)? {
                                session.give(r)?;
                            }
                        }
                    }
                    Ok(())
                })
            }
        })
    }

    fn flat_map_with_name<O, R, F>(self, name: &str, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        R: Iterator<Item = O> + Send + 'static,
        F: Fn(I) -> FnResult<R> + Send + 'static,
    {
        self.unary(&_get_name("flat_map", name), |info| {
            let index = info.index;
            move |input, output| {
                let mut eval_time = 0_u128;
                let mut batch_time = 0_u128;
                let mut send_time0 = 0_u128;
                let mut send_time1 = 0_u128;
                let mut chunk_time = 0_u128;
                let mut push_time = 0_u128;
                let start = Instant::now();
                let result = input.for_each_batch(|dataset| {
                    let batch_start = Instant::now();
                    if !dataset.is_empty() {
                        let mut session = output.new_session(&dataset.tag)?;
                        let mut data_vec = vec![];
                        let push_start: Instant = Instant::now();
                        for item in dataset.drain() {
                            data_vec.push(item);
                        }
                        push_time += push_start.elapsed().as_nanos();
                        let chunk_start = Instant::now();
                        for item in data_vec.drain(..) {
                        // for item in dataset.drain() {
                            let eval_start = Instant::now();
                            let iter = func(item)?;
                            eval_time += eval_start.elapsed().as_nanos();
                            let send_start = Instant::now();
                            if let Err(err) = session.give_iterator(iter) {
                                if err.is_would_block() || err.is_interrupted() {
                                    trace_worker!("flat_map_{} is blocked on {:?};", index, session.tag,);
                                }
                                send_time1 += send_start.elapsed().as_nanos();
                                return Err(err)?;
                            }
                            send_time0 += send_start.elapsed().as_nanos();
                        }
                        chunk_time += chunk_start.elapsed().as_nanos();
                    }
                    batch_time += batch_start.elapsed().as_nanos();
                    Ok(())
                });
                let total_time = start.elapsed().as_nanos() as f64 / 1e9;
                let eval_time = eval_time as f64 / 1e9;
                let batch_time = batch_time as f64 / 1e9;
                let send_time0 = send_time0 as f64 / 1e9;
                let send_time1 = send_time1 as f64 / 1e9;
                let chunk_time = chunk_time as f64 / 1e9;
                let push_time = push_time as f64 / 1e9;
                if index == 3 || index == 4 {
                    println!(
                        "flat_map_{} eval time: {:?}, total time: {:?}, batch time: {}, send time: {} / {}, chunk time {}, push time {}",
                        index, eval_time, total_time, batch_time, send_time0, send_time1, chunk_time, push_time
                    );
                }
                result
            }
        })
    }
}
