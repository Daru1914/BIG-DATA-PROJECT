USE projectdb;                                                                                                                                                          
                                                                                                                                                                        
INSERT OVERWRITE LOCAL DIRECTORY '/root/q3'                                                                                                                             
 ROW FORMAT DELIMITED                                                                                                                                                   
  FIELDS TERMINATED BY ','                                                                                                                                              
   SELECT r.name AS restaurant_name, m.name AS menu_item_name, m.price AS menu_item_price
    FROM restaurants r
    JOIN menus m ON r.id = m.restaurant_id
    ORDER BY CAST(REPLACE(m.price, 'USD', '') AS FLOAT) DESC
    LIMIT 10;