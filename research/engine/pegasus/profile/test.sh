scale_factor=$1
query=$2
worker=$3

export PEGASUS_HOME="/home/graphscope/GraphScope/research/engine/pegasus"
export BINARY_DATA_PATH="/run/run/ldbc_bin"

export QUERY_PATH="/home/graphscope/query"

export DATA_PATH=/run/run/ldbc_bin/ldbc_${scale_factor}_bin/

datetime=$(date +%Y%m%d%H%M%S)
log_file=temp_${scale_factor}_${query}_${datetime}

RUST_LOG=INFO ${PEGASUS_HOME}/benchmark/target/release/ldbc -w ${worker} -q ${QUERY_PATH}/sf${scale_factor}/${query} &> ${log_file}
python summary.py ${log_file} ${worker}
