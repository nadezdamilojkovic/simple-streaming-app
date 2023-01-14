#!/bin/sh
if [ -z "${BOOTSTRAP_SERVERS}" ]; then
  echo "BOOTSTRAP_SERVERS variable not set"
  exit 1
else
  BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS}"
fi

if [ -z "${REPLICATION_FACTOR}" ]; then
  echo "Setting default replication factor to 1..."
  REPLICATION_FACTOR=1
else
  REPLICATION_FACTOR="${REPLICATION_FACTOR}"
fi

if [ -z "${PARTITIONS}" ]; then
  echo "Setting default number of partitions to 9..."
  PARTITIONS=1
else
  PARTITIONS="${PARTITIONS}"
fi

# Name of the topics to initialize on the broker
TOPICS=(
  "input-stream"
  "per-second-metrics-stream"
  "per-minute-metrics-stream"
  "output-stream"
)
INPUT_TOPIC=${TOPICS[0]}
CREATE_TOPIC_SCRIPT_PATH="kafka-topics"
PRODUCE_DATA_SCRIPT_PATH="kafka-console-producer"


for TOPIC in "${TOPICS[@]}"
do
  echo "Creating topic: $TOPIC..."
  ${CREATE_TOPIC_SCRIPT_PATH} \
  --create --if-not-exists \
  --bootstrap-server ${BOOTSTRAP_SERVERS} \
  --replication-factor ${REPLICATION_FACTOR} \
  --partitions ${PARTITIONS} \
  --topic ${TOPIC}
done

echo "Producing data from $INPUT_DATA_PATH to topic: $INPUT_TOPIC..."
${PRODUCE_DATA_SCRIPT_PATH} \
 --broker-list ${BOOTSTRAP_SERVERS} \
 --topic ${INPUT_TOPIC} \
 --property value.serializer=custom.class.serialization.JsonSerializer \
 < ${INPUT_DATA_PATH}

echo "Sucessfully produced data to topic: $TOPIC!"
