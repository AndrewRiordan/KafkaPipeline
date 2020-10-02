#!/bin/sh
mvn clean compile assembly:single
docker image build -f producer.df -t csv_producer:latest .
docker image build -f consumer.df -t csv_consumer:latest .
