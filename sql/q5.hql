USE projectdb;                                                                                                                                                          
                                                                                                                                                                        
INSERT OVERWRITE LOCAL DIRECTORY '/root/q5'                                                                                                                             
ROW FORMAT DELIMITED                                                                                                                                                    
 FIELDS TERMINATED BY ','                                                                                                                                               
  SELECT zip_code, COUNT(*) AS num_restaurants                                                                                                                          
   FROM restaurants                                                                                                                                                     
    GROUP BY zip_code                                                                                                                                                   
     ORDER BY num_restaurants DESC;