USE projectdb;                                                                                                                                                          
                                                                                                                                                                        
INSERT OVERWRITE LOCAL DIRECTORY '/root/q9'                                                                                                                             
 ROW FORMAT DELIMITED                                                                                                                                                   
  FIELDS TERMINATED BY ','                                                                                                                                              
   SELECT r.name, AVG(CAST(r.score AS FLOAT)) AS avg_rating                                                                                                             
    FROM restaurants r                                                                                                                                                  
     JOIN menus m ON r.id = m.restaurant_id                                                                                                                             
      WHERE LOWER(m.name) LIKE '%burger%'                                                                                                                               
       GROUP BY r.name                                                                                                                                                  
        ORDER BY avg_rating DESC;