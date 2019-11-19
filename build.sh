#!/usr/bin/env bash

# this script just builds the application 
# and copies spark.properties from resources to some 
# directory where it is picked up by compute.sh script
#
# You have to figure out how to run that on your own,
# This script is good for my environment

mvn clean install && \
    cp job/src/main/resources/spark.properties ../../compose/submit/ && \
    cp job/target/build.jar ../../compose/submit
