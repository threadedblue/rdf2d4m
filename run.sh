#!/usr/bin/env bash

DIR=`pwd`
cd rdf2d4m
java -jar rdf2d4m-0.0.1.jar
#   -i /ccd/rdf -ow -fl --instance accumulo
cd $DIR