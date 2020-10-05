package com.viasat.pipeline;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.Properties;

public class Kafka_Producer extends Thread {
    String broker;
    Properties props;
    org.apache.kafka.clients.producer.KafkaProducer producer;
    String topic;
    private Schema tschema;
    public Kafka_Producer(String broker, String schema, String topic) {
        try {
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    org.apache.kafka.common.serialization.StringSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    KafkaAvroSerializer.class);
            properties.put("schema.registry.url", "http://schema-registry:8081");
            producer = new KafkaProducer<>(properties);
            Schema.Parser parser = new Schema.Parser();
            this.topic = topic;
            tschema = parser.parse("{\"type\":\"record\"," +
                    "\"name\":\"transaction\"," +
                    "\"fields\":[{\"name\":\"userid\",\"type\":\"int\"}," +
                    "{\"name\":\"Subreddit\",\"type\":\"string\"}]}");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void push(Object[] fields) {
        GenericRecord record = new GenericData.Record(tschema);
        for (int i = 0; i < fields.length; i++) {
            record.put(tschema.getFields().get(i).name(), fields[i]);
        }
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>(topic, record.get("userid").toString(), record);
        producer.send(producerRecord, null);
    }
}
