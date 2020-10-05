package com.viasat.pipeline;

import java.io.*;

import static java.lang.Boolean.valueOf;

public class CSVProducer extends Kafka_Producer {
    String infile;
    public CSVProducer(String broker, String infile, String schema, String topic) {
        super(broker, schema, topic);
        this.infile = infile;
    }

    public void start() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(infile));
            String row;
            int k = 0;
            while((row=br.readLine()) != null) {

                k++;
                String[] topics = row.split(",");
                String userid = topics[0];
                for (int i = 1; i < topics.length; i++) {
                        Object[] fields = new Object[2];
                        fields[0] = Integer.parseInt(userid);
                        fields[1] = topics[i];
                        this.push(fields);
                    }
                }
            producer.flush();
        } catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println("closed producer all data sent");
        producer.close();
    }
}
