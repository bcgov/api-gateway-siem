#!/bin/sh

start-cluster.sh

flink run target/flink-jobs-1.0-SNAPSHOT-jar-with-dependencies.jar