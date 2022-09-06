package com.start.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

import static net.logstash.logback.argument.StructuredArguments.kv;

public class ProducerDemoWithCallbackAndKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbackAndKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello world");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; ++i) {
            // create a producer record - this is the data to be sent to topic
            String topic = "demo_java1";
            String key = "id_" + i;
            String value = "hello world from Java " + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            // send data - asynchronous
            producer.send(producerRecord, new Callback() { // this is an async operation
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (Objects.nonNull(e)) {
                        log.error("Error while producing", e);
                    } else {
                        log.info("{} {} {} {} {}",
                                kv("Key", producerRecord.key()),
                                kv("Topic", recordMetadata.topic()),
                                kv("Partition", recordMetadata.partition()),
                                kv("Offset", recordMetadata.offset()),
                                kv("TimeStamp", recordMetadata.timestamp()));
                    }
                }
            });
        }

        // flush data - synchronous operation
        producer.flush(); // this will wait until all the data is send to the topic

        //flush and close the producer
        producer.close();

        // before running this code we need to create topic demo_java from CLI
    }
}