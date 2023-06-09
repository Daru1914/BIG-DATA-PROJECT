USE projectdb;                                                                                                                                                          
                                                                                                                                                                        
INSERT OVERWRITE LOCAL DIRECTORY '/root/q10'                                                                                                                            
ROW FORMAT DELIMITED                                                                                                                                                    
 FIELDS TERMINATED BY ','                                                                                                                                               
  SELECT r.name, m.name, m.price                                                                                                                                        
   FROM restaurants r                                                                                                                                                   
    JOIN menus m ON r.id = m.restaurant_id                                                                                                                              
     ORDER BY CAST(regexp_replace(m.price, 'USD', '') AS FLOAT) DESC                                                                                                    
             LIMIT 10;