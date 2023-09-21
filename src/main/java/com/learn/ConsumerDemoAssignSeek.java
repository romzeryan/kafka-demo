package com.learn;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {

  private static final Logger log = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

  public static void main(String[] args) {
    log.info("Consumer Assign Seek demo");

    String bootstrapServers = "127.0.0.1:9092";
    String topic = "demo_java";

    // create consumer configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    TopicPartition partition0 = new TopicPartition(topic, 0);
    consumer.assign(List.of(partition0));

    int seekOffset = 7;
    consumer.seek(partition0, seekOffset);

    boolean keepOnPolling = true;
    int numberOfMessagesToRead = 5;
    int numberOfMessagesReadSoFar = 0;

    while (keepOnPolling) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      for (ConsumerRecord<String, String> record : records) {
        numberOfMessagesReadSoFar += 1;

        log.info("Key: " + record.key() + ", Value: " + record.value());
        log.info("Partition: " + record.partition() + ", Offset:" + record.offset());

        if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
          keepOnPolling = false; // to exit the while loop
          break; // to exit the for loop
        }
      }
    }

    consumer.close();
    log.info("Exiting the application");
  }

}
