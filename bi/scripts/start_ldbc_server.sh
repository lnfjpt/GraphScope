#!/bin/bash

CSR_PATH=$1
SERVER_CONFIG_PATH=$2
QUERIES_CONFIG_PATH=$3
PROXY_ENDPOINT=$4

pushd . > /dev/null

cd "$( cd "$( dirname "${BASH_SOURCE[0]}}" )" >/dev/null 2>&1 && pwd )"
cd ..

RUST_LOG=info GraphScope/interactive_engine/executor/server/target/release/start_ldbc_server --graph_data ${CSR_PATH} --queries_config ${QUERIES_CONFIG_PATH} --servers_config ${SERVER_CONFIG_PATH}

popd > /dev/null