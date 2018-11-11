#!/bin/bash

fname=$1

if [ -f $fname ]; then # if the file exists

    mvn clean install
    docker build -t mshtelma/yelp-ingest-pipeline:latest .
    mkdir /tmp/spark
    mkdir /tmp/yelp_dataset
    tar xf $fname -C /tmp/yelp_dataset
    docker service create \
    --mount type=bind,source=/tmp/yelp_dataset,target=/data  \
    --mount type=bind,source=/tmp/spark,target=/temp  \
    --network yelpstack_sparknet \
    --publish 4040:4040  \
    --restart-condition none  \
    mshtelma/yelp-ingest-pipeline:latest
fi


