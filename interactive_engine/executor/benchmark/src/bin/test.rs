use pegasus::api::{EmitKind, IterCondition, Iteration, Map, Reduce, Sink};
use pegasus::{Configuration, JobConf};
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
}

fn main() {
    let config: Config = Config::from_args();

    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();

    let mut conf = JobConf::new("iteration-test");
    conf.set_workers(config.workers);
    conf.plan_print = true;

    pegasus::run(conf, || {
        move |input, output| {
            let stream = input.input_from(vec![0i64])?;
            stream
                .filter_map(move |source| if source > 0 { Ok(Some(source)) } else { Ok(None) })?
                .reduce(|| |a, _| Ok(a))?
                .unfold(|a| Ok(Some(a).into_iter()))?
                .iterate_emit_until(IterCondition::max_iters(64), EmitKind::After, move |start| {
                    start.map(|count| Ok(count + 1))
                })?
                .sink_into(output)
        }
    })
    .expect("submit test job failure");
    pegasus::shutdown_all();
}
