package solacestream;

import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import events.Broker;
import events.AppSingleton;
import events.EventListener;

import java.util.concurrent.ConcurrentLinkedQueue;



// import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
// import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}

import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.Row

import org.apache.spark.sql.sources.DataSourceRegister

import org.apache.spark.sql.types._



class BasicOffset(aPartition: String, aOffset: Int) extends Offset{
  val logger = LoggerFactory.getLogger(classOf[BasicOffset])
  var partition:String = aPartition;
  var offset:Int = aOffset
//  logger.info("BasicOffset Constructor: " + aPartition + "," + aOffset + ")")
      
  def json(): String = {
    val mystr = "{\"partition\":" + partition + ",\"offset\":" + offset + "}";
    //logger.info("JSON: " + mystr);
    return mystr;
  }
  override def toString():String = {
    return("BasicOffset[" + partition + "," + offset + "]");
  }
  override def equals(aObj:Any):Boolean = {
//    logger.info("equals:" + this.toString() + ":" + aObj.toString());
    val obj = aObj.asInstanceOf[BasicOffset]
    if (this.offset == obj.offset  && this.partition.equals(obj.partition))
      return true;
    else
      return false;
  }
}


class DefaultSource extends DataSourceV2 with ReadSupport with MicroBatchReadSupport {
  val logger = LoggerFactory.getLogger(classOf[DefaultSource])
  //logger.info("Hello from the SimpleApp class")

  def createReader(options: DataSourceOptions): DataSourceReader = {
    System.out.println("createReader");
    new SimpleDataSourceReader()
  }

  
  def createMicroBatchReader(schema: java.util.Optional[StructType], checkpointLocation: String, options: DataSourceOptions): MicroBatchReader = {
    logger.info("createMicroBatchReader:" + schema + ":" + checkpointLocation + ":" + options.asMap());
    


//  	val eventListener = new EventListener();
//  	AppSingleton.getInstance().setCallback(eventListener);    
//    val broker = new Broker();
//    broker.setReceiver(eventListener);

    
//    val e:EventListener = AppSingleton.getInstance().getCallback();
//  	e.debugIt();

    
		return new BasicMicroBatchDataSourceReader(schema, checkpointLocation, options);
  }
}


class SimpleDataSourceReader extends DataSourceReader {
    val logger = LoggerFactory.getLogger(classOf[SimpleDataSourceReader])

  def readSchema() = StructType(Array(StructField("value", StringType)))

  def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
		System.out.println("planInputPartitions");		

		
		val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]();
		factoryList.add(new SimpleDataSourceReaderFactory(0,2));
//		factoryList.add(new SimpleDataSourceReaderFactory(7,8));
//		factoryList.add(new SimpleDataSourceReaderFactory(3,4));
//		factoryList.add(new SimpleDataSourceReaderFactory(5,6));
		return(factoryList);
  }
}

class BasicMicroBatchDataSourceReader(schema: java.util.Optional[StructType], checkpointLocation: String, options: DataSourceOptions) extends MicroBatchReader {
    val logger = LoggerFactory.getLogger(classOf[BasicMicroBatchDataSourceReader])
    var more = 0;

    //logger.info("BasicMicroBatchDataSourceReader:" + schema + ":" + checkpointLocation + ":" + options.asMap());

//  		log.info("setOffsetRange:" + start + "," + end);	
		
//		this.start = start.isPresent()?start.get():null;
//		this.end = end.isPresent()?end.get():null;
//		this.start = new BasicOffset(0, 1);
//		this.end = new BasicOffset(3, 3);

	var start: BasicOffset = null;
	var end: BasicOffset = null;
	initialize();
	
	def initialize():Unit = synchronized{
  	System.out.println("Starting The DataLoader Now!!!!");
  
	}
	
	
  def commit(end: Offset): Unit = {
    //logger.info("Commit: " + end)
  }
  
  def deserializeOffset(json: String): Offset = {
    //logger.info("deserializeOffset: " + json)
    return new BasicOffset("one",20);
  }

  def getEndOffset(): Offset = {
    //logger.info("getEndOffset: " +  end.toString())
    end
  }

  def getStartOffset(): Offset = {
    //logger.info("getStartOffset: " +  start.toString())
    start
  }
  
  def setOffsetRange(astart: java.util.Optional[Offset], aend: java.util.Optional[Offset]): Unit = {
    //logger.info("setOffsetRange: " + astart +" : " + aend + "(more=" + more + ")" );
    start = new BasicOffset("one",0);
    end = new BasicOffset("one",20+more);
    more += 1;
    
  }
  
  def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
		//logger.info("planInputPartitions");	
    
//    val queue = new ConcurrentLinkedQueue[String]()
//    
//    queue.add("ABCD");
//    var q = queue.poll()
//    if (q != null){
//      logger.info("Found Element " + q);
//    }
//    else{
//      logger.info("Q Empty");      
//    }
//    q = queue.poll()
//    if (q != null){
//      logger.info("Found Element " + q);
//    }
//    else{
//      logger.info("Q Empty");      
//    }

    
    


    
		val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]();
		factoryList.add(new SimpleDataSourceReaderFactory(0,2));
//		factoryList.add(new SimpleDataSourceReaderFactory(7,8));
//		factoryList.add(new SimpleDataSourceReaderFactory(3,4));
//		factoryList.add(new SimpleDataSourceReaderFactory(5,6));
		return(factoryList);
  }

//  def readSchema() = StructType(Array(StructField("VAL", StringType)))
  
//  def readSchema() = StructType(
//        StructField("VAL", StringType)
//  )
  
  def readSchema() =
   StructType(
     StructField("firstName", StringType, false) ::
     StructField("lastName", StringType, false) ::
     StructField("amount", DoubleType, false) ::
     StructField("cardType", StringType, false) ::
     StructField("cardNumber", StringType, false) :: Nil)

  

  def stop(): Unit = {
        logger.info("STOP!")
  }
}

class SimpleDataSourceReaderFactory(aStart:Int, aEnd:Int) extends InputPartition[InternalRow] {
  val logger = LoggerFactory.getLogger(classOf[SimpleInputPartitionReader])
   //logger.info("SimpleDataSourceReaderFactory");		

  val start = aStart
  val end = aEnd

  def createPartitionReader(): InputPartitionReader[InternalRow] = {
    		//logger.info("createPartitionReader");		
    		new SimpleInputPartitionReader(start, end);
  }
}


class SimpleInputPartitionReader(aStart:Int, aEnd:Int) extends InputPartitionReader[InternalRow] {
  val logger = LoggerFactory.getLogger(classOf[SimpleInputPartitionReader])
  //logger.info("SimpleInputPartitionReader");		

  logger.info("Creating connection");
	val eventListener = new EventListener();
	AppSingleton.getInstance().setCallback(eventListener);    
  val broker = new Broker();
  broker.setReceiver(eventListener);
  logger.info(	AppSingleton.getInstance().debits.toString());

  

  
  val start = aStart
  val end = aEnd
  var index = start

  val values = Array("1", "2", "3", "4", "5","6","7","8","9","10")



    
  def next(): Boolean = {
      !AppSingleton.getInstance().debits.isEmpty();     
    
    //logger.info("NEXT: "+ index)
    //index < end
  }
  
  def get(): InternalRow = { 
     logger.info("GET:(" + start + ") " + index)
    
      val q = AppSingleton.getInstance().debits.poll();     
      if (q != null){
        logger.info("Found Element " + q);
//        val row = InternalRow.fromSeq(Seq(UTF8String.fromString(start + ":" + values(index))))
        val row = InternalRow.fromSeq(
            Seq(
                UTF8String.fromString(q.getFirstName()),
                UTF8String.fromString(q.getLastName()),
                q.getAmount(),
                UTF8String.fromString(q.getCardType()),
                UTF8String.fromString(q.getCardNumber()),
                UTF8String.fromString(q.getCustNumber()),
                UTF8String.fromString(q.getMobile())
//                UTF8String.fromString(start + ":" + values(index))
            )
         )
        index = index + 1
        logger.info(row.toString())
        row
      }
      else{
        logger.info("Q Empty");    
        null;
      }

    
  }
  
  def close(): Unit = {
    logger.info("CLOSE!")
  }

}