package com.viasat.pipeline;

import java.io.*;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static java.lang.Boolean.valueOf;

public class CSVProducer extends Thread {
    String broker;
    String infile;
    Properties props;
    KafkaProducer producer;
    String topic;
    private final Schema transSchema;
    public CSVProducer(String broker, String infile, String schema, String topic) {
        this.broker = broker;
        this.infile = infile;
        this.topic = topic;
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.IntegerSerializer.class);
       // props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
         //       io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class);
        props.put("schema.registry.url", schema);
        producer = new KafkaProducer(props);
        Schema.Parser parser = new Schema.Parser();

        transSchema = parser.parse("{\"type\":\"record\"," +
                "\"name\":\"transaction\"," +
                "\"fields\":[{\"name\":\"Userid\",\"type\":\"int\"}," +
                "{\"name\":\"Subreddit\",\"type\":\"string\"}]}");
    }

    public void start() {
        PrintStream ps = null;
        BufferedReader br = null;
        try {
            ps = new PrintStream(new File("errorlogoops.txt"));
            ps.println("start of log");
            br = new BufferedReader(new FileReader(infile));
            String row;
            int k = 0;
            while((row=br.readLine()) != null) {

                k++;
                String[] topics = row.split(",");
                String userid = topics[0];
                for (int i = 1; i < topics.length; i++) {
                        /*
                        String cross;
                        if (topics[i].compareTo(topics[j]) < 0){
                            cross = topics[i] + "|" + topics[j];
                        } else {
                            cross = topics[j] + "|" + topics[i];
                        }
                        */
                        GenericRecord trans = new GenericData.Record(transSchema);
                        trans.put("Userid", Integer.parseInt(userid));
                        trans.put("Subreddit", topics[i]);
                        ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, trans.get("Userid"), (Object)trans);
                        producer.send(record);
                    }
                }
            producer.flush();
        } catch(Exception e) {
            System.out.println("ree");
            e.printStackTrace(ps);
        }
        System.out.println("closed producer all data sent");
        producer.close();
    }
}
