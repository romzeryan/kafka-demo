package com.learn;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

  private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

  public static void main(String[] args) {
    log.info("I am a Kafka Consumer");

    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "my-fifth-application";
    String topic = "demo_java";

    // create consumer configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    ConsumerRebalanceListenerImpl rebalanceListener = new ConsumerRebalanceListenerImpl(consumer);

    // get a reference to the current thread
    final Thread mainThread = Thread.currentThread();

    // adding the shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
      consumer.wakeup();

      // join the main thread to allow the execution of the code in the main thread
      try {
        mainThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }));

    // subscribe consumer to our topic(s)
    consumer.subscribe(List.of(topic), rebalanceListener);

    try {
      // poll for new data
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
          log.info("Key: " + record.key() + ", Value: " + record.value());
          log.info("Partition: " + record.partition() + ", Offset:" + record.offset());

          // we track the offset we have been committed in the listener
          rebalanceListener.addConsumerOffset(topic, record.partition(), record.offset());
        }

        // We commitAsync as we have processed all data, and we don't want to block until the next .poll() call
        consumer.commitAsync();
      }
    } catch (WakeupException e) {
      log.info("Wake up exception!");
      // we ignore this as this is an expected exception when closing a consumer
    } catch (Exception e) {
      log.error("Unhandled exception", e);
    } finally {
      try {
        consumer.commitSync(rebalanceListener.getCurrentOffsets());
      } finally {
        consumer.close();
        log.info("Consumer has been gracefully closed");
      }
    }

  }

}
