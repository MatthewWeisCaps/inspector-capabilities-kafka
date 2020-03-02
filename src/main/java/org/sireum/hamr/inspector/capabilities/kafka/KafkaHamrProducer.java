package org.sireum.hamr.inspector.capabilities.kafka;

import art.DataContent;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

class KafkaHamrProducer {

    private final KafkaProducer<String, String> producer = createProducer();
    private final String topic;

    public KafkaHamrProducer(String topic) {
        this.topic = topic;
    }

    void send(int src, int dst, long time, String dataContent) {
        // todo optimize space (repeating timestamps, useless keys, etc.
        producer.send(new ProducerRecord<String, String>(topic, 0, time, "src", Integer.toString(src)));
        producer.send(new ProducerRecord<String, String>(topic, 1, time, "dst", Integer.toString(dst)));
        producer.send(new ProducerRecord<String, String>(topic, 2, time, "data", dataContent));
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("producer.type", "async");

        properties.put("acks", "all"); // if we want to acknowledge producer requests
        properties.put("retries", 0);

        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(properties);
    }

}
