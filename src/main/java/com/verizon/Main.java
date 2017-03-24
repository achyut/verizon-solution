/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.verizon;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

// Import Row.
import org.apache.spark.sql.Row;
import scala.Tuple2;


/**
 *
 * @author gokarna
 */
public class Main {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    
    String warehouseLocation = "file:" + System.getProperty("user.dir") + "spark-warehouse";
    
    SparkSession spark = SparkSession
      .builder()
      .appName("Verizon")
      .config("spark.master","local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate();
    
    Configuration configuration = new Configuration();
    configuration.addResource(new Path(System.getProperty("HADOOP_INSTALL")+"/conf/core-site.xml"));
    configuration.addResource(new Path(System.getProperty("HADOOP_INSTALL")+"/conf/hdfs-site.xml"));
    configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    configuration.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
    
    FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
    

    SQLContext context = new SQLContext(spark);
    String schemaString = " Device,Title,ReviewText,SubmissionTime,UserNickname";
    //spark.read().textFile(schemaString)
    Dataset<Row> df = spark.read().csv("hdfs://localhost:9000/data.csv");
    //df.show();
    //#df.printSchema();
    df = df.select("_c2");
    
    Path file = new Path("hdfs://localhost:9000/tempFile.txt");
    if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
 
    df.write().csv("hdfs://localhost:9000/tempFile.txt");
    
    JavaRDD<String> lines = spark.read().textFile("hdfs://localhost:9000/tempFile.txt").javaRDD();
    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) {
        return Arrays.asList(SPACE.split(s)).iterator();
      }
    });

    JavaPairRDD<String, Integer> ones = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) {
            s = s.replaceAll("[^a-zA-Z0-9]+","");
            s = s.toLowerCase().trim();
          return new Tuple2<>(s, 1);
        }
      });

    JavaPairRDD<String, Integer> counts = ones.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });
    
     JavaPairRDD<Integer,String> frequencies = counts.mapToPair( 
        new PairFunction< 
                         Tuple2<String, Integer>,      
                         Integer,                       
                         String                     
                        >() { 
        @Override 
        public Tuple2<Integer,String> call(Tuple2<String, Integer> s) { 
            return new Tuple2<Integer,String>(s._2, s._1); 
        } 
    });
     
    frequencies = frequencies.sortByKey(false);
    
     JavaPairRDD<String, Integer> result = frequencies.mapToPair(
      new PairFunction<Tuple2<Integer,String>, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(Tuple2<Integer, String> s) throws Exception {
            return new Tuple2<String,Integer>(s._2, s._1);
        }
        
      });
         
    //JavaPairRDD<Integer,String> sortedByFreq = sort(frequencies, "descending"); 
    file = new Path("hdfs://localhost:9000/allresult.csv");
    if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 

    
    //FileUtils.deleteDirectory(new File("allresult.csv"));
    
    result.saveAsTextFile("hdfs://localhost:9000/allresult.csv");
    
    List<Tuple2<String, Integer>> output = result.take(250);
    
    ExportToHive hiveExport = new ExportToHive();
    String rows = "";
    for (Tuple2<String,Integer> tuple : output) {
        String date = new Date().toString();
        String keyword = tuple._1();
        Integer count = tuple._2();
        //System.out.println( keyword+ "," +count);
        rows += date+","+"Samsung Galaxy s7,"+keyword+","+count+System.lineSeparator();
        
    }
    //System.out.println(rows);
    /*
    file = new Path("hdfs://localhost:9000/result.csv");
    
    if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
    OutputStream os = hdfs.create(file);
    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
    br.write(rows);
    br.close();
    */
hdfs.close();
       
 FileUtils.deleteQuietly(new File("result.csv"));
    FileUtils.writeStringToFile(new File("result.csv"), rows);
    
    hiveExport.writeToHive(spark);
    ExportDataToServer exportServer = new ExportDataToServer();
    exportServer.sendDataToRESTService(rows);
    spark.stop();
  }
  
 
}
