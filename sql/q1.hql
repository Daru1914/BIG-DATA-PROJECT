USE projectdb;                                                                                                                                                          
                                                                                                                                                                        
INSERT OVERWRITE LOCAL DIRECTORY '/root/q1'                                                                                                                             
ROW FORMAT DELIMITED                                                                               
FIELDS TERMINATED BY ','   
SELECT category, COUNT(*) AS num_restaurants            
FROM restaurants    
GROUP BY category      
ORDER BY num_restaurants DESC;