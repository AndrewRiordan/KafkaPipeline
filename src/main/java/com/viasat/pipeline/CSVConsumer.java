package com.viasat.pipeline;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sun.net.www.content.text.Generic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Properties;

public class CSVConsumer {
    String broker;
    String infile;
    KafkaConsumer kc;
    String topic;
    public CSVConsumer(String broker, String outfile, String schema, String topic) {
        this.broker = broker;
        this.infile = outfile;
        this.topic = topic;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        //props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        //                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        //        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        //                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", schema);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kc = new KafkaConsumer<Integer, String>(props);
    }

    public void start() {
        FileWriter bw = null;
        try {
            bw = new FileWriter(infile);
            String row;
            int k = 0;
            try {
                kc.subscribe(Arrays.asList(topic));
                while (true) {
                    System.out.println("polling");
                    ConsumerRecords<Integer, GenericRecord> records = kc.poll(100);
                    System.out.println("poll exited");
                    for (ConsumerRecord<Integer, GenericRecord> record : records) {
                        //System.out.printf("offset = %d,x key = %s, value = %s \n", record.offset(), record.key(), record.value());
                        bw.append(record.value().get("Userid") + "," + record.value().get("Subreddit") + "\n");
                        bw.flush();
                    }
                }
            } finally {
                kc.close();
                bw.close();
            }
        } catch (Exception e) {
            System.out.println("ree");
            e.printStackTrace();
        }

    }

}