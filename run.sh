#!/bin/bash
###### TODO: REFER THE SPARK UI AND INSERT/CONFIGURE YOUR PARAMETERS ######

spark-submit \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    --conf spark.driver.memory=5g \
    --num-executors=3 \
    --executor-memory=11g \
    --executor-cores=3 \
    --conf spark.default.parallelism=10 \
    --class PageRank target/project_spark.jar wasb://datasets@clouddeveloper.blob.core.windows.net/iterative-processing/Graph wasb://datasets@clouddeveloper.blob.core.windows.net/iterative-processing/Graph-Topics wasbs:///pagerank-output wasbs:///recs-output

# Follow the instructions in the writeup to configure the parameters given carefully. 

# You should be able to hit the performance benchmark that we have set with the parameters and optimizations that were already suggested in the writeup. If you want to further tweak your performance, you can look into tuning the Spark configurations, refer: https://spark.apache.org/docs/2.3.0/configuration.html