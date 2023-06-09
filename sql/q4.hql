USE projectdb;                                                                                                                                                          
                                                                                                                                                                        
INSERT OVERWRITE LOCAL DIRECTORY '/root/q4'                                                                                                                             
 ROW FORMAT DELIMITED                                                                                                                                                   
  FIELDS TERMINATED BY ','                                                                                                                                              
   SELECT name, COUNT(*) AS num_occurrences                                                                                                                             
    FROM menus                                                                                                                                                          
     GROUP BY name                                                                                                                                                      
      ORDER BY num_occurrences DESC                                                                                                                                     
       LIMIT 10;