#!/bin/bash

apt update
apt install -y \
  ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev vim wget \
  git g++ libgoogle-glog-dev libopenmpi-dev default-jdk libcrypto++-dev \
  libboost-all-dev libxml2-dev curl protobuf-compiler zstd zip
apt install -y xfslibs-dev libgnutls28-dev liblz4-dev maven openssl pkg-config \
  libsctp-dev gcc make python3 systemtap-sdt-dev libtool libyaml-cpp-dev \
  libc-ares-dev stow libfmt-dev diffutils valgrind doxygen python3-pip net-tools

export RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static
export RUSTUP_UPDATE_ROOT=https://mirrors.ustc.edu.cn/rust-static/rustup

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
rustup default 1.72.0

pip3 install click

mkdir /opt/cmake
pushd /opt/cmake
sudo wget http://ldbc-snb-bi.oss-cn-shanghai.aliyuncs.com/cmake-3.25.0-rc1-Linux-x86_64.tar.gz
tar -zxvf cmake-3.25.0-rc1-Linux-x86_64.tar.gz --strip-components=1
echo 'export PATH=/opt/cmake/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
popd

wget http://ldbc-snb-bi.oss-cn-shanghai.aliyuncs.com/grpc.tar.gz
tar zxf grpc.tar.gz
mkdir grpc/cmake/build
pushd grpc/cmake/build
cmake -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF ../..
make -j 4
make install
popd

pushd . > /dev/null
cd ..
cd GraphScope/interactive_engine/compiler
make build
popd > /dev/null