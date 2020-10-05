package com.viasat.pipeline;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sun.net.www.content.text.Generic;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;

public class CSVConsumer extends Kafka_Consumer{
    PrintWriter pw;

    public CSVConsumer(String broker, String outfile, String schema, String topic) {
        super(broker, schema, topic);
        try {
            pw = new PrintWriter(outfile);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.infile = outfile;

    }
    public void onMessage(long offset, String key, GenericRecord value) {
            int userid = (int) value.get("userid");
            String subreddit = (String)value.get("subreddit");
            pw.println(userid + "," + subreddit + "\n");
            pw.flush();
    }


}