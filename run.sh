#!/bin/bash

spark-submit \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.autoBroadcastJoinThreshold=10485760 \
    --conf spark.driver.memory=6g \
    --num-executors=5 \
    --executor-memory=9g \
    --executor-cores=7 \
    --conf spark.default.parallelism=70 \
    --conf spark.storage.memoryFraction=0.6 \
    --class PageRank target/project_spark.jar wasb://datasets@clouddeveloper.blob.core.windows.net/iterative-processing/Graph wasb://datasets@clouddeveloper.blob.core.windows.net/iterative-processing/Graph-Topics wasbs:///pagerank-output wasbs:///recs-output