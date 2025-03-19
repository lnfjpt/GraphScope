HOSTFILE=$1
OUTPUT_DIR=$2
PEGASUS_PORT=$3
RPC_PORT=$4
PROXY_PORT=$5
WORKER_NUM=$6
SERVER_SIZE=$7

if [[ -d "$OUTPUT_DIR" ]]; then
    echo "output dir \"$OUTPUT_DIR\" already exists"
    exit 1
else
    mkdir -p $OUTPUT_DIR
fi

for i in $(seq 0 $((SERVER_SIZE - 1))); do
    echo "[network_config.network]" >> $OUTPUT_DIR/server$i.toml
    echo "server_id = $i" >> $OUTPUT_DIR/server$i.toml
    echo "servers_size = $SERVER_SIZE" >> $OUTPUT_DIR/server$i.toml
    echo "" >> $OUTPUT_DIR/server$i.toml
done

readarray -t hostlist < ${HOSTFILE}
for i in "${!hostlist[@]}"; do
    for j in $(seq 0 $((SERVER_SIZE - 1))); do
        echo "[[network_config.network.servers]]" >> $OUTPUT_DIR/server$j.toml
        echo "hostname = '${hostlist[$i]}'" >> $OUTPUT_DIR/server$j.toml
	echo "port = $PEGASUS_PORT" >> $OUTPUT_DIR/server$j.toml
        echo "" >> $OUTPUT_DIR/server$j.toml
    done
done

for i in "${!hostlist[@]}"; do
    echo "[rpc_server]" >> $OUTPUT_DIR/server$i.toml
    echo "rpc_host = \"${hostlist[$i]}\"" >> $OUTPUT_DIR/server$i.toml
    echo "rpc_port = $RPC_PORT" >> $OUTPUT_DIR/server$i.toml
    echo "" >> $OUTPUT_DIR/server$i.toml
    echo "[http_server]" >> $OUTPUT_DIR/server$i.toml
    echo "http_host = \"${hostlist[$i]}\"" >> $OUTPUT_DIR/server$i.toml
    echo "http_port = $PROXY_PORT" >> $OUTPUT_DIR/server$i.toml
    echo "" >> $OUTPUT_DIR/server$i.toml
    echo "[pegasus_config]" >> $OUTPUT_DIR/server$i.toml
    echo "worker_num=$WORKER_NUM" >> $OUTPUT_DIR/server$i.toml
done

exit 0