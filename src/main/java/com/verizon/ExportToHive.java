/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.verizon;

import org.apache.spark.sql.SparkSession;

/**
 *
 * @author gokarna
 */
public class ExportToHive {
    
    public void writeToHive(SparkSession spark){
      
        spark.sql("DROP TABLE IF EXISTS result");
        //spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS result (Date:String, Model:String, Keyword:STRING, Counts:INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '/result.csv'");
        spark.sql("CREATE TABLE IF NOT EXISTS result (Date:String, Model:String, Keyword:STRING, Counts:INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");
        
        spark.sql("LOAD DATA LOCAL INPATH 'result.csv' INTO TABLE result");
        //String sql = "INSERT INTO TABLE result VALUES('"+DATE+"','SAMSUNG GALAXY','"+KEYWORD+"','"+COUNTS+"')";
        //System.out.println(sql);
        //spark.sql(sql);
        
        // Queries are expressed in HiveQL
        spark.sql("SELECT * FROM result").show();
        
    }
}
