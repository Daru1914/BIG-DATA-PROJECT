USE projectdb;                                                                                                                                                          
                                                                                                                                                                        
INSERT OVERWRITE LOCAL DIRECTORY '/root/q3'                                                                                                                             
 ROW FORMAT DELIMITED                                                                                                                                                   
  FIELDS TERMINATED BY ','                                                                                                                                              
   SELECT category, AVG(price_range) AS avg_price_range                                                                                                                 
    FROM restaurants                                                                                                                                                    
     GROUP BY category                                                                                                                                                  
      ORDER BY avg_price_range DESC;