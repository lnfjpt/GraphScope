#!/bin/bash

QUERIES_PATH=$1

pushd . > /dev/null

cd "$( cd "$( dirname "${BASH_SOURCE[0]}}" )" >/dev/null 2>&1 && pwd )"
cd ..

SF=10
VALIDATION=true

# path to the directory containing the update configuration files with name `delete-*.json` and `insert-*json`
UPDATE_CONFIG_DIR=/path/to/update_config/

# path to update dir containing subdirectories with name `inserts` and `deletes` 
UPDATE_DIR=/path/to/update_dir/


java -classpath driver/target/ldbc-bi-driver-0.1.0.jar -DSF=${SF} -Dqueries=${QUERIES_PATH} -Dparameters=${QUERIES_PATH} -Dendpoint=127.0.0.1:12345 -Dengine=BI -Dworkers=8 -Dvalidation=${VALIDATION} -DupdateConfigDir=${UPDATE_CONFIG_DIR} -DupdateDir=${UPDATE_DIR} com.alibaba.graphscope.ldbc.workload.Benchmark

popd > /dev/null