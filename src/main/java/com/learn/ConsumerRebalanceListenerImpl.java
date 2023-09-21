package com.learn;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumerRebalanceListenerImpl implements ConsumerRebalanceListener {

  private static final Logger log = LoggerFactory.getLogger(ConsumerRebalanceListenerImpl.class);

  private final KafkaConsumer<String, String> consumer;

  private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

  ConsumerRebalanceListenerImpl(KafkaConsumer<String, String> consumer) {
    this.consumer = consumer;
  }

  public void addConsumerOffset(String topic, int partition, long offset) {
    currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset + 1));
  }

  // this is used when we shut down our consumer gracefully
  public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
    return currentOffsets;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    log.info("onPartitionsRevoked callback triggered");
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    log.info("onPartitionsAssigned callback triggered");
  }

}
