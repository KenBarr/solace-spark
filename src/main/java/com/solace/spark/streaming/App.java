package com.solace.spark.streaming;


import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class App {
	private SparkSession spark;
	
	
	public App() {
		spark = SparkSession.
				builder()
				.appName("Simple Application")
				.config("spark.master", "local[2]")
				.getOrCreate()
				;

		
	}
	
    public static void main(String[] args) throws StreamingQueryException {
    	App app = new App();
    	
//    	app.test1();
//    	app.test6();
    	app.test7();
    }
    
    
    void test1() throws StreamingQueryException {
    	log.info("==========test1===============");

    	// Create DataFrame representing the stream of input lines from connection to host:port
	    Dataset<Row> lines = spark
	      .readStream()
	      .format("socket")
	      .option("host", "localhost")
	      .option("port", 9999)
	      .load();

	    System.out.println("lines schema");
	    lines.printSchema();

	    
	    // Split the lines into words
	    Dataset<String> words = lines.as(Encoders.STRING()).flatMap(
	        (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
	        Encoders.STRING());

	    System.out.println("words schema");
	    words.printSchema();

	    
	    // Generate running word count
	    Dataset<Row> wordCounts = words.groupBy("value").count();

	    System.out.println("wordCounts schema");
	    wordCounts.printSchema();

	    
	    // Start running the query that prints the running counts to the console
	    StreamingQuery query = wordCounts.writeStream()
//	      .outputMode("complete")
//	      .format("console")
	      .start();

	    query.awaitTermination();
    	
    	
    }
    
	void test6() {
		log.info("======= test6 ===========");

		Dataset<Row> peopleDF =
				spark.read().format("com.solace.spark.streaming.basic")
				.option("hosturi","tcp://vmr-mr3e5sq7dacxp.messaging.solace.cloud:20480")
				.option("username","solace-cloud-client")
				.option("password","cge4fi7lj67ms6mnn2b4pe76g2")
				.option("vpn","msgvpn-8ksiwsp0mtv")
				.option("topic","a/b")
				.load();
		log.info("peopleDF Streaming : " + peopleDF.isStreaming());

		
		log.info("printSchema");
		peopleDF.printSchema();
		log.info("show");
		peopleDF.show();
		
		Dataset<Row>  streamingSelectDF = 
				peopleDF
				    .groupBy("a") 
				    .count();
//		streamingSelectDF.show();
		
		log.info("number of partitions in simple multi source is "+peopleDF.rdd().getNumPartitions());
			
	}

	void test7() throws StreamingQueryException {
		log.info("======= test7 ===========");

		
		
		
		Dataset<Row> peopleDF =
				spark.readStream().format("com.solace.spark.streaming.basic")
				.option("hosturi","tcp://vmr-mr3e5sq7dacxp.messaging.solace.cloud:20480")
				.option("username","solace-cloud-client")
				.option("password","cge4fi7lj67ms6mnn2b4pe76g2")
				.option("vpn","msgvpn-8ksiwsp0mtv")
				.option("topic","a/b")
				.load();
		log.info("peopleDF Streaming : " + peopleDF.isStreaming());
		
	    Dataset<Row> wordCounts = peopleDF.groupBy("a").count();
		log.info("wordCounts Streaming : " + wordCounts.isStreaming());

	    StreamingQuery query = wordCounts.writeStream()
	  	      .outputMode("update")
	  	      .format("console")
//	  	      .trigger(Trigger.Continuous(500)) 
	  	      .start();
		log.info("wordCounts Streaming : " + wordCounts.isStreaming());
	    
	    query.awaitTermination();

			
	}

    
    
    
}
