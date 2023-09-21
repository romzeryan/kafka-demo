package com.learn;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

  public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

  public static void main(String[] args) throws InterruptedException {
    log.info("Hello, I'm Kafka Producer");

    String bootstrapServers = "127.0.0.1:9092";

    // create Producer properties
    Properties properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(LINGER_MS_CONFIG, String.valueOf(50));

    // create the producer
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

      for (int i = 0; i < 30; i++) {
        String key = "id_" + i;

        // create a producer record
        ProducerRecord<String, String> record =
            new ProducerRecord<>("demo_java", key,"message#" + i);

        producer.send(record, (metadata, exception) -> {
          if (exception == null) {
            // the record was successfully sent
            log.info("Received new metadata. \n" +
                "Topic:" + metadata.topic() + "\n" +
                "Key:" + record.key() + "\n" +
                "Partition: " + metadata.partition() + "\n" +
                "Offset: " + metadata.offset() + "\n" +
                "Timestamp: " + metadata.timestamp());
          } else {
            log.error("Error while producing", exception);
          }
        });
//        Thread.sleep(1000);
      }
      producer.flush();

    }
  }

}
