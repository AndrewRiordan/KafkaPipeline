package com.viasat.pipeline;

public class Main {

    public static final String BROKER = "broker:29092";
    public static final String SCHEMA_REGISTRY = "http://schema-registry:8081";
    public static final String OUTFILE = "/datafiles/output_data.csv";
    public static final String INFILE = "/datafiles/reddituserpostingbehavior.csv";
    public static final String topic = "topic3";
    public static void main(String[] args) {
        if (args[0].equalsIgnoreCase("consume")) {
            CSVConsumer consumer = new CSVConsumer(BROKER, OUTFILE, SCHEMA_REGISTRY, topic);
            consumer.start();
        }
        else {
            CSVProducer producer = new CSVProducer(BROKER, INFILE, SCHEMA_REGISTRY, topic);
            producer.start();
        }
    }

}
