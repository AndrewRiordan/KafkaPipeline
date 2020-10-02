#!/bin/sh
docker run --mount type=bind,src=$(pwd)/datafiles,dst=/datafiles --link broker:broker --link schema-registry:schema-registry --net base_default csv_consumer
