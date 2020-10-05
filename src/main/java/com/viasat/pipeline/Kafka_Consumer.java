package com.viasat.pipeline;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.FileWriter;
import java.util.Arrays;
import java.util.Properties;

public abstract class Kafka_Consumer extends Thread {
    String broker;
    String infile;
    org.apache.kafka.clients.consumer.KafkaConsumer kc;
    String topic;
    public Kafka_Consumer(String broker, String schema, String topic) {
        try {
            this.broker = broker;
            this.topic = topic;
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
            props.put("schema.registry.url", "http://schema-registry:8081");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            kc = new org.apache.kafka.clients.consumer.KafkaConsumer<Integer, String>(props);
        } catch (Exception e ) {
            e.printStackTrace();
        }
    }
    protected abstract void onMessage(long offset, String key, GenericRecord value);

    public void start() {
            try {
                kc.subscribe(Arrays.asList(topic));
                while (true) {
                    System.out.println("polling");
                    ConsumerRecords<Integer, GenericRecord> records = kc.poll(100);
                    System.out.println("poll exited");
                    for (ConsumerRecord<Integer, GenericRecord> record : records) {
                        onMessage(record.offset(), "" + record.key(), record.value());
                    }
                }
            } finally {
                kc.close();
            }

    }

}