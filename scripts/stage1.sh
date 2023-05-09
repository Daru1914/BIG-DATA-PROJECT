#!/bin/bash

psql -U postgres -f sql/db.sql
hdfs dfs -rm -r /project/restaurants
hdfs dfs -rm -r /project/menus   
sqoop import-all-tables -Dmapreduce.job.user.classpath.first=true --connect jdbc:postgresql://localhost/project --username postgres --warehouse-dir /project --as-avrodatafile --autoreset-to-one-mapper --compression-codec=snappy --outdir /project/avsc --m 1