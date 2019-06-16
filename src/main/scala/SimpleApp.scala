import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import hello._
import events.Broker;

object SimpleApp {
  def main(args: Array[String]) {

    val logger = LoggerFactory.getLogger("classOf[SimpleApp]")
    logger.info("Hello from the SimpleApp class")
    
    val broker = new Broker();
//    broker.sendMessage
    

//    val sparktry1 =  SparkTry1
//    sparktry1.setup
    // sparktry1.try1
    // sparktry1.try2
    // sparktry1.try3
    // sparktry1.try4
    // sparktry1.try5
//    sparktry1.try6
//    sparktry1.try7
//
//    sparktry1.shutdown

    // val o1 = SparkSQLExample
    // o1.main()

    Thread.sleep(1000000);

    
  }
  
}