USE projectdb;                                                                                                                                                          
                                                                                                                                                                        
INSERT OVERWRITE LOCAL DIRECTORY '/root/q6'                                                                                                                             
ROW FORMAT DELIMITED                                                                                                                                                    
 FIELDS TERMINATED BY ','                                                                                                                                               
  SELECT DISTINCT r.name                                                                                                                                                
   FROM restaurants r                                                                                                                                                   
    JOIN menus m ON r.id = m.restaurant_id                                                                                                                              
     WHERE LOWER(m.name) LIKE '%pizza%';