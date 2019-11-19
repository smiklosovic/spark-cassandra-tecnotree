#!/usr/bin/env bash

docker exec -i spark_master_1 sh -c '${SPARK_HOME}/bin/spark-submit \
    --driver-memory 3G \
    --num-executors 2 \
    --executor-memory 3G \
    --total-executor-cores 4 \
    --class com.instaclustr.TecnotreeApp \
    --master spark://spark-master-1:7077 \
    --deploy-mode client \
    --properties-file /submit/spark.properties \
    --verbose \
    /submit/build.jar'
