#!/usr/bin/env bash

cd ../accumulo.access/
gradle clean install
cd ../accumulo.d4m/
gradle clean install
cd ../rdf2d4m/
gradle  --refresh-dependencies clean bundle