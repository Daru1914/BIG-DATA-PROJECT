USE projectdb;                                                                                                                                                          
                                                                                                                                                                        
INSERT OVERWRITE LOCAL DIRECTORY '/root/q2'                                                                                                                             
 ROW FORMAT DELIMITED                                                                                                                                                   
  FIELDS TERMINATED BY ','                                                                                                                                              
   SELECT category, name, score                                                                                                                                         
    FROM restaurants                                                                                                                                                    
     WHERE score IS NOT NULL                                                                                                                                            
      ORDER BY score DESC                                                                                                                                               
      LIMIT 10;