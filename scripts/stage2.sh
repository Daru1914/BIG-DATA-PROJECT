#!/bin/bash                                                                                                                                                             
                                                                                                                                                                        
hadoop fs -mkdir -p /project/avsc && cd avsc && hdfs dfs -put *.avsc /project/avsc                                                                                      
                                                                                                                                                                        
cd ..                                                                                                                                                                   
                                                                                                                                                                        
hive -f sql/db.hql                                                                                                                                                      
                                                                                                                                                                        
hive -f sql/q1.hql                                                                                                                                                      
echo "category,num_restaurants" > output/q1.csv                                                                                                                         
cat /root/q1/* >> output/q1.csv
python scripts/file_transformer.py output/q1.csv                                                                                                                                         
                                                                                                                                                                        
hive -f sql/q2.hql                                                                                                                                                      
echo "category,name,score" > output/q2.csv                                                                                                                              
cat /root/q2/* >> output/q2.csv  
python scripts/file_transformer_2.py output/q2.csv                                                                                                                                           
                                                                                                                                                                        
hive -f sql/q3.hql                                                                                                                                                      
echo "category,avg_price_range" > output/q3.csv                                                                                                                         
cat /root/q3/* >> output/q3.csv   
python scripts/file_transformer.py output/q3.csv                                                                                                                                        
                                                                                                                                                                        
hive -f sql/q4.hql                                                                                                                                                      
echo "name,num_occurences" > output/q4.csv                                                                                                                              
cat /root/q4/* >> output/q4.csv                                                                                                                                         
                                                                                                                                                                        
hive -f sql/q5.hql                                                                                                                                                      
echo "zip_code, num_restaurants" > output/q5.csv                                                                                                                        
cat /root/q5/* >> output/q5.csv                                                                                                                                         
                                                                                                                                                                        
hive -f sql/q6.hql                                                                                                                                                      
echo "name" > output/q6.csv                                                                                                                                             
cat /root/q6/* >> output/q6.csv                                                                                                                                         
                                                                                                                                                                        
hive -f sql/q7.hql                                                                                                                                                      
echo "name,category,avg_price" > output/q7.csv                                                                                                                          
cat /root/q7/* >> output/q7.csv                                                                                                                                         
                                                                                                                                                                        
hive -f sql/q8.hql                                                                                                                                                      
echo "category,count" > output/q8.csv                                                                                                                                   
cat /root/q8/* >> output/q8.csv                                                                                                                                         
                                                                                                                                                                        
hive -f sql/q9.hql
echo "name,avg_rating" > output/q9.csv                                                                                                                                  
cat /root/q9/* >> output/q9.csv                                                                                                                                         
                                                                                                                                                                        
hive -f sql/q10.hql                                                                                                                                                     
echo "restaurant_name,dish_name,price" > output/q10.csv                                                                                                                 
cat /root/q10/* >> output/q10.csv