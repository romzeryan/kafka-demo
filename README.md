## Kafka playground

#### Setup 
1. Download the latest version of Apache Kafka from https://kafka.apache.org/downloads under Binary downloads.
2. Start zookeeper:   
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
3. Start Kafka: 
```
bin/kafka-server-start.sh config/server.properties
```

#### Topics

* Create topic:   
```
kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --create --partitions 3 --replication-factor 1
```

* Produce messages:
```
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic
```

* Consume messages in group:
```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-application 
```