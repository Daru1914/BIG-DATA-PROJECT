USE projectdb;                                                                                                                                                          
                                                                                                                                                                        
INSERT OVERWRITE LOCAL DIRECTORY '/root/q8'                                                                                                                             
ROW FORMAT DELIMITED                                                                                                                                                    
 FIELDS TERMINATED BY ','                                                                                                                                               
  SELECT m.category, COUNT(*) AS count                                                                                                                                  
   FROM restaurants r                                                                                                                                                   
    JOIN menus m ON r.id = m.restaurant_id                                                                                                                              
     GROUP BY  m.category                                                                                                                                               
      ORDER BY count DESC                                                                                                                                               
       LIMIT 5;