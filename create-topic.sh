bin/kafka-topics.sh --create --topic $1 --bootstrap-server localhost:9092 \
--config remote.storage.enable=true --config local.retention.ms=1000 --config retention.ms=180000 \
--config segment.bytes=1048576 --config file.delete.delay.ms=1000
