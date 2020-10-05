#!/bin/sh
docker run --rm --mount type=bind,src=$(pwd)/datafiles,dst=/datafiles --link broker:broker --link schema-registry:schema-registry --net cp-all-in-one_default csv_producer
