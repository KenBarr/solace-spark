package hello

import org.apache.spark.sql.SparkSession

// $example on:programmatic_schema$
import org.apache.spark.sql.Row
// $example off:programmatic_schema$
// $example on:init_session$
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
// $example off:data_types$
// $example off:programmatic_schema$





object SparkTry1 extends App {
  println("Hello, SparkTry1")
  var spark : SparkSession = null

  case class Person(name: String, age: Long)

  def setup {
    println("setup");
    val spark2 = SparkSession.builder.appName("Simple Application").master("local[3]").getOrCreate()
    import spark2.implicits._
    spark = spark2
  }

  def try1 {
    val logFile = "README.md" // Should be some file on your system
    println(spark.version)
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    println(spark.version)
  }


  def try2{
    val myspark = spark
    import myspark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    println("=======11111111")
    println("Streaming" + lines.isStreaming)
    lines.printSchema
    println("=======11111111")

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    println("=======22222")
    println("Streaming" + wordCounts.isStreaming)
    wordCounts.printSchema
    println("=======22222222")

    println("=======33333")
    wordCounts.createOrReplaceTempView("try2")
    val sqlDF = spark.sql("SELECT * FROM try2 where count > 2" )
    val query2 = sqlDF.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    println("=======3333333")


    // Start running the query that prints the running counts to the console
    // val query = wordCounts.writeStream
    //   .outputMode("complete")
    //   .format("console")
    //   .start()



  
    query2.awaitTermination()
    println("=======4444")

    // query.awaitTermination()
  }


  def try3{
    val df = spark.read.json("people.json")
    df.show()
    df.createOrReplaceTempView("people")

  }

  def try4{
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()


  }

  def try5 {
    val myspark = spark

    import myspark.implicits._

    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    val path = "people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
  }

  def shutdown {
    spark.stop()

  }


  def try6{
    val myspark = spark
    import myspark.implicits._

    val simpleDf = spark.read
              .format("solacestream")
              .load()
      simpleDf.show()
  }
  def try7{
    val myspark = spark
    import myspark.implicits._

    val simpleDf = spark.readStream
              .format("solacestream")
              .load()

    val wordCounts = simpleDf.groupBy("value").count()
         
     val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
             
              
    query.awaitTermination()
  }

}