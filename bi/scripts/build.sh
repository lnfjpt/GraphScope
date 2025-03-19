#!/bin/bash

use_mimalloc=false

for arg in "$@"; do
  if [ "$arg" = "--use_mimalloc" ]; then
    use_mimalloc=true
    break
  fi
done

pushd . >/dev/null

cd "$(cd "$(dirname "${BASH_SOURCE[0]}}")" >/dev/null 2>&1 && pwd)"
cd ..

echo "Start build driver"
pushd driver >/dev/null
mvn clean package
popd >/dev/null

echo "Start build storage"
pushd GraphScope/interactive_engine/executor/store/bmcsr >/dev/null
cargo build --release
popd >/dev/null

echo "Start build runner"
pushd GraphScope/interactive_engine/executor/server >/dev/null
if $use_mimalloc; then
  cargo build --release --features "use_mimalloc_rust"
else
  cargo build --release
fi
popd >/dev/null

echo "Start http proxy"
pushd GraphScope/interactive_engine/executor/proxy >/dev/null
cargo build --release
popd >/dev/null

echo "Start build codegen"
mkdir -p build
pushd build >/dev/null
cmake ..
make -j
popd >/dev/null

popd >/dev/null
