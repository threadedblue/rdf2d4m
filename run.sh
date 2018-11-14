#!/usr/bin/env bash

DIR=`pwd`
NAME="rdf2d4m"
cd ${NAME}
java -jar ${NAME}-0.0.1.jar -i /rdf -t 500rdfSPO -fs hdfs://haz00:9000 -l accumulo -zk haz00:2181 -ow -r -c file:///home/haz/accumulo-creds.yml
cd $DIR