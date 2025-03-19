HOSTFILE=$1
OUTPUT_PATH=$2
PROXY_PORT=$3

rm $OUTPUT_PATH
echo "{ \"servers\": [" >> $OUTPUT_PATH
for i in `cat $HOSTFILE`; do
    echo "{" >> $OUTPUT_PATH
    echo "\"host\": \"http://$i\"," >> $OUTPUT_PATH
    echo "\"port\": $PROXY_PORT" >> $OUTPUT_PATH
    echo "}," >> $OUTPUT_PATH
done
sed -i '$ s/,$//' $OUTPUT_PATH
echo "] }" >> $OUTPUT_PATH

echo "Driver configuration file has been created"
exit 0