#!/bin/bash                                                                                                                                                             
spark-submit --jars /usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,/usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar --packa
ges org.apache.spark:spark-avro_2.12:3.0.3 scripts/model.py                                                                                                             
hdfs dfs -get /project/output/lr_predictions.csv/part-00000-f4054b24-5337-4de2-9123-2f67ba5f9746-c000.csv output/lr_predictions.csv                                     
hdfs dfs -get /project/output/dtr_predictions.csv/part-00000-de037e84-a288-4434-aede-9dd97dffaacd-c000.csv output/dtr_predictions.csv  