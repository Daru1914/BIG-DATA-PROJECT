#!/bin/bash                          
echo "spark.driver.memory 7g" >> /etc/spark2/conf/spark-defaults.conf                                                                                                                                    
spark-submit --jars /usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,/usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar --packa
ges org.apache.spark:spark-avro_2.12:3.0.3 scripts/model.py
rm -r output/lr_pred                                                                                                                                                    
rm -r output/dtr_pred                                                                                                                                                   
mkdir output/lr_pred                                                                                                                                                    
mkdir output/dtr_pred                                                                                                                                                   
hdfs dfs -get /project/output/lr_predictions.csv/*.csv output/lr_pred/                                                                                                  
hdfs dfs -get /project/output/dtr_predictions.csv/*.csv output/dtr_pred/    