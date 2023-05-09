USE projectdb;                                                                                                                                                          
                                                                                                                                                                        
INSERT OVERWRITE LOCAL DIRECTORY '/root/q7'                                                                                                                             
ROW FORMAT DELIMITED                                                                                                                                                    
 FIELDS TERMINATED BY ','                                                                                                                                               
  SELECT r.name, m.category, AVG(m.price) AS avg_price                                                                                                                  
    FROM restaurants r                                                                                                                                                  
     JOIN menus m ON r.id = m.restaurant_id                                                                                                                             
      WHERE r.name = 'Golden Temple Vegetarian Cafe'                                                                                                                    
                     GROUP BY r.name, m.category;