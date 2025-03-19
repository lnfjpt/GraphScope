#!/bin/bash

CYPHER_INPUT_PATH=$1
CYPHER_INPUT_PATH=$(readlink -f "$CYPHER_INPUT_PATH")

PLAN_INPUT_PATH=$2
if [ -n "$PLAN_INPUT_PATH" ]; then
  PLAN_INPUT_PATH=$(readlink -f "$PLAN_INPUT_PATH")
fi

pushd . >/dev/null
cd "$(cd "$(dirname "${BASH_SOURCE[0]}}")" >/dev/null 2>&1 && pwd)"
cd ..

if [ -d "dylib" ]; then
  rm -rf dylib/*
else
  mkdir -p dylib
fi
echo "[workspace]" >>dylib/Cargo.toml
echo "members = [" >>dylib/Cargo.toml

echo "queries:" >>dylib/queries.yaml

for i in $(ls CYPHER_INPUT_PATH/*.cypher); do
  query_name=$(basename "$i" .cypher)
  echo $query_name
  echo "\"$query_name\"," >>dylib/Cargo.toml

  mkdir -p dylib/$query_name
  sed "s/\${query_name}/$query_name/g" "example/Cargo.toml.template" >"dylib/$query_name/Cargo.toml"
  mkdir -p dylib/$query_name/src
  property_path="example/ir.compiler.properties"
  if [[ $query_name == insert* || $query_name == delete* || $query_name == *precompute ]]; then
    mode="write"
    write_yaml="false"
  else
    mode="read"
    write_yaml="true"
  fi
  java -cp ~/temp/gie/GraphScope/interactive_engine/compiler/target/compiler-0.0.1-SNAPSHOT-shade.jar \
    com.alibaba.graphscope.common.ir.tools.GraphPlanner ${property_path} \
    $i dylib/$query_name/plan dylib/$query_name/config.yaml ${write_yaml}
  build/gen_pegasus_from_plan -i dylib/$query_name/plan -n $query_name -t plan -s \
    GraphScope/interactive_engine/executor/store/bmcsr/schema.json -r single_machine \
    -o dylib/$query_name/src/lib.rs -m ${mode}
  echo "  - name: \"$query_name\"" >>dylib/queries.yaml
  echo "    description: \"$query_name\"" >>dylib/queries.yaml
  echo "    extension: \".so\"" >>dylib/queries.yaml
  echo "    library: \"/path/lib${query_name}.so\"" >>dylib/queries.yaml
  if [ "$write_yaml" = "true" ]; then
    python3 scripts/parse_query_info.py dylib/$query_name/config.yaml dylib/queries.yaml
  else
    echo "    params:" >> dylib/queries.yaml
    echo "      - name: \"csv_path\"" >> dylib/queries.yaml
    echo "        type: \"string\"" >> dylib/queries.yaml
  fi
done

if [ -n "$PLAN_INPUT_PATH" ]; then
  for i in $(ls $PLAN_INPUT_PATH/*.json); do
    query_name=$(basename "$i" .json)
    echo $query_name
    echo "\"$query_name\"," >> dylib/Cargo.toml

    mkdir -p dylib/$query_name
    sed "s/\${query_name}/$query_name/g" "example/Cargo.toml.template" > "dylib/$query_name/Cargo.toml"
    mkdir -p dylib/$query_name/src
    if [[ $query_name == insert* || $query_name == delete* || $query_name == *precompute ]]; then
        mode="write"
    else
        mode="read"
    fi
    build/gen_pegasus_from_plan -i $i -n $query_name -t json -s \
    GraphScope/interactive_engine/executor/store/bmcsr/schema.json -r single_machine \
    -o dylib/$query_name/src/lib.rs -m ${mode}
  done
fi

echo "]" >>dylib/Cargo.toml

pushd dylib
cargo build --release --features "use_mimalloc_rust"
popd

popd >/dev/null
