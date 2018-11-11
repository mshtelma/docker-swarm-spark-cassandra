#!/usr/bin/env bash

mkdir /tmp/yelp_dataset
docker stack deploy -c docker-compose.yml yelpstack