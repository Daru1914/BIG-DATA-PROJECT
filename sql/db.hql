DROP DATABASE IF EXISTS projectdb CASCADE;                                                                                                                              
                                                                                                                                                                        
CREATE DATABASE projectdb;                                                                                                                                              
USE projectdb;                                                                                                                                                          
SET mapreduce.map.output.compress = true;                                                                                                                               
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;                                                                                    
                                                                                                                                                                        
CREATE EXTERNAL TABLE restaurants STORED AS AVRO LOCATION '/project/restaurants' TBLPROPERTIES ('avro.schema.url'='/project/avsc/restaurants.avsc');                    
                                                                                                                                                                        
CREATE EXTERNAL TABLE menus STORED AS AVRO LOCATION '/project/menus' TBLPROPERTIES ('avro.schema.url'='/project/avsc/menus.avsc');                                      
                                                                                                                                                                        
SET hive.exec.dynamic.partition = true;                                                                                                                                 
SET hive.exec.dynamic.partition.mode = nonstrict;                                                                                                                       
                                                                                                                                                                        
CREATE EXTERNAL TABLE menus_part(                                                                                                                                       
        category varchar(2000),                                                                                                                                         
        name varchar(2000),                                                                                                                                             
        description varchar(10000),                                                                                                                                     
        price varchar(20))                                                                                                                                              
PARTITIONED BY (restaurant_id int) STORED AS AVRO LOCATION '/project/menus_part' TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');                                              
                                                                                                                                                                        
SET hive.enforce.bucketing=true;                                                                                                                                        
                                                                                                                                                                        
INSERT INTO menus_part partition (restaurant_id) SELECT category, name, description, price, restaurant_id FROM menus LIMIT 100000;