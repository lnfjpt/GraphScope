## Install dependency
```shell
git clone git@github.com:GraphScope/gie-codegen.git -b ln_dev
cd gie-codegen
scripts/install_dependency.sh
source ~/.bashrc
```
## Build Pegasus
```shell
git submodule udpate --init
cd GraphScope && git checkout ln_dev && cd ..
scripts/build.sh --use_mimalloc
```
## Generate dylib
## Prepare
1. 下载数据

raw data
```shell
export DATASET_URL=https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/bi-sf10-composite-projected-fk.tar.zst
curl --silent --fail ${DATASET_URL} | tar -xv --use-compress-program=unzstd
```
parameters
```shell
curl https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/parameters-2022-10-01.zip \
parameters-2022-10-01.zip
unzip parameters-2022-10-01.zip
```
2. Load Graph
```shell
input_path="/data/bi-sf10-composite-projected-fk/graphs/csv/bi/composite-projected-fk/initial_snapshot"
output_path="/data/ldbc_sf10_p1"
RUST_LOG=info gie-codegen/GraphScope/interactive_engine/executor/store/target/release/build_bmcsr_partition \
${input_path} ${output_path} gie-codegen/GraphScope/interactive_engine/executor/store/bmcsr/input.json \
gie-codegen/GraphScope/interactive_engine/executor/store/bmcsr/schema.json -p 1 -i 0 --skip_header
```
3. 生成查询的动态库

目前支持通过从cypher查询和从Physical Plan两种方式生成动态库
+ 将cypher文件和physical plan文件分别放置在两个目录中
```shell
cypher_dir="cypher_dir"
plan_dir="physical_plan"
scripts/gen_dylib.sh ${cypher_dir} ${plan_dir}
```
4. 生成查询的配置
查询配置在`example/queries.yaml`
```shell
queries:
  - name: "insert_comment"
    description: "insert_comment"
    mode: "WRITE"
    extension: ".so"
    library: "/path/libinsert_comment.so"
    params:
      - name: "csv_path"
        type: "string"
    ...
```
将该文件内配置的so路径替换成对应动态库的路径
## Start Server
```shell
binary_data_path="/data/ldbc_bmcsr_10/"
server_config="example/server.toml"
query_config="example/queries.yaml"
proxy_endpoint="127.0.0.1:12346"
scripts/start_ldbc_server.sh ${binary_data_path} ${server_config} ${query_config} ${proxy_endpoint}
```
## Run Benchmark
1. Power Test
```shell
java -classpath driver/target/ldbc-bi-driver-0.1.0.jar -DSF=10 -Dparameters=/data/parameters/parameters-sf10 \
-Dendpoint=127.0.0.1:12346 -Dworkers=8 -Draw_data=/data/bi-sf10-composite-projected-fk/graphs/csv/bi/composite-projected-fk/ \
com.alibaba.graphscope.ldbc.workload.Benchmark
```
2. Throughput Test
```shell
java -classpath driver/target/ldbc-bi-driver-0.1.0.jar -DSF=10 -Dparameters=/data/parameters/parameters-sf10 \
-Dendpoint=127.0.0.1:12346 -Dworkers=8 -Draw_data=/data/bi-sf10-composite-projected-fk/graphs/csv/bi/composite-projected-fk/ \
-DqueryMode=throughput com.alibaba.graphscope.ldbc.workload.Benchmark
```
3. Validation Test
sf10的正确性结果路径为`example/results.csv`
```shell
java -classpath driver/target/ldbc-bi-driver-0.1.0.jar -DSF=10 -Dparameters=example/results.csv \
-Dendpoint=127.0.0.1:12346 -Dworkers=8 -Draw_data=/data/bi-sf10-composite-projected-fk/graphs/csv/bi/composite-projected-fk/ \
-Dvalidation=true com.alibaba.graphscope.ldbc.workload.Benchmark
```