package org.kafkainaction.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class AsiaCreditConsumer {

  final static Logger log = LoggerFactory.getLogger(AsiaCreditConsumer.class);

  private volatile boolean keepConsuming = true;

  public static void main(String[] args) {
    Properties kaProperties = new Properties();  //<1>
    kaProperties.put("bootstrap.servers", "localhost:9092");
    kaProperties.put("group.id", "kinaction_helloconsumer");
    kaProperties.put("enable.auto.commit", "true");
    kaProperties.put("auto.commit.interval.ms", "1000");
    kaProperties.put("key.deserializer",
              "org.apache.kafka.common.serialization.StringDeserializer");
    kaProperties.put("value.deserializer",
              "org.apache.kafka.common.serialization.StringDeserializer");

    AsiaCreditConsumer helloWorldConsumer = new AsiaCreditConsumer();
    helloWorldConsumer.consume(kaProperties);
    Runtime.getRuntime().addShutdownHook(new Thread(helloWorldConsumer::shutdown));
  }

  private void consume(Properties kaProperties) {
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kaProperties)) {
      consumer.subscribe(List.of("topic1"));  //<2>

      while (keepConsuming) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));  //<3>
        for (ConsumerRecord<String, String> record : records) {   //<4>
          log.info("kinaction_info offset = {}, kinaction_value = {}",
                   record.offset(), record.value());
        }
      }
    }
  }

  private void shutdown() {
    keepConsuming = false;
  }
}
