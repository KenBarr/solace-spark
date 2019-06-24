package solacestream

import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import hello._
import events.Broker;
import events.AppSingleton;
import events.EventListener;

import payment.Debit;


//public class MyCallback implements Callable{
//  
//  public void MyMethod(){
//    
//  }
//}
//
//
//public interface Callable{
//  
//}


object StructuredApp {
  var spark : SparkSession = null
  val logger = LoggerFactory.getLogger("StructuredApp")


  def callback(debit:Debit):Unit = {
    logger.info("callback")
    
  }
  
  def setup {
    println("setup");
    
//  	val eventListener = new EventListener();
//  	AppSingleton.getInstance().setCallback(eventListener);    
//    val broker = new Broker();
//    broker.setReceiver(eventListener);
    

    val spark2 = SparkSession.builder
      .appName("StructuredApp ")
      .master("local[2]")
//      .config("spark.executor.instances", "1")
//      .config("spark.executor.cores", "1")
      .getOrCreate()
    import spark2.implicits._
    spark = spark2
    
    //set new runtime options
//    spark.conf.set("spark.sql.shuffle.partitions", 6)
//    spark.conf.set("spark.executor.memory", "2g")
    //get all settings
//    val configMap:Map[String, String] = spark.conf.getAll()
//    logger.info("=========================");
//    logger.info(configMap);
    
  }

  def shutdown {
    spark.stop()

  }

  def processor{
    val myspark = spark
    import myspark.implicits._

    
//      val simpleDf = spark.read
//              .format("solacestream")
//              .load()
//      simpleDf.show()

    
    val simpleDf = spark.readStream
              .format("solacestream")
              .load()

    simpleDf.printSchema
    simpleDf.createOrReplaceTempView("simpleDf")

    val sqlDf = spark
              .sql("SELECT firstName, count(firstName), sum(amount) from simpleDF group by firstName")
    sqlDf.printSchema

              
//    val wordCounts = simpleDf.groupBy("firstName")
//      .count()
//    
//    wordCounts.printSchema

         
     val query = sqlDf.writeStream
      .outputMode("complete")
      .format("console")
      .start()
             
              
    query.awaitTermination()
  }

  
  def main(args: Array[String]) {

    logger.info("Application starting")
    
    setup
    processor
    shutdown    
    Thread.sleep(100000);
    
  }
  
}