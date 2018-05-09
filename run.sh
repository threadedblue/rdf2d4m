#!/usr/bin/env bash

DIR=`pwd`
cp -r rdf2d4m ~/
cd ~/rdf2d4m
java -jar rdf2d4m-0.0.1.jar -i /scap2rdf/arf.rdf -o ec2 -ow -c file://conf/rdf2d4m.yml
cd $DIR