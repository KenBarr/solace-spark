### README

## Environment Setup

# Solace

Defaults used in the code

http://localhost:8080

Create a Queue _*payment/card*_ in the _default_ VPN

Subscribe the queue to the topic _payment/tx_. This is the topic that is currently used by the simulator

# Spark
The checkpoint directory is currently configured as _./tmp/abcd_


## Running

gradle run

The program will start and display the streaming SQL output for every microbatch


-------------------------------------------<br/>
Batch: 1<br/>
-------------------------------------------<br/>
+---------+----------------+-----------+<br/>
|firstName|count(firstName)|sum(amount)|<br/>
+---------+----------------+-----------+<br/>


Once the Spark program is running start the simulator as well

[simulator ]$ gradle run --args "Pankaj Arora 100"

You can see that the  streaming AVRO data is processed

Batch: 168<br/>
-------------------------------------------<br/>
+---------+----------------+-----------+<br/>
|firstName|count(firstName)|sum(amount)|<br/>
+---------+----------------+-----------+<br/>
|   Pankaj|               1|      100.0|<br/>
+---------+----------------+-----------+<br/>



## Explanation

The program uses Structured Streaming with DataSourceV2. This example is only doing READ and can be extended to WRITE as well.

THe program uses checkpoints and hence even if the program crashes and you restart it the streaming data is retained and you dont need to replay any data.

The program works from the command line or can be submitted as a spark job. That will require the FAT Jar and can be created using
gradle shadowJar

Program Flow
( @todo clean up the code and remove the unncessary files)

_StructuredApp.scala_ is the driver. The processor reads the stream from package 'solacestream' which is all defined in _MyBasicStreamingSource.scala_

DataSourceV2 is a marker interface and implemented by DefaultSource
DefaultSource also implements the MicroBatchReadSupport which is required for Streaming read in micro-batches.

createMicroBatchReader returns the BasicMicroBatchDataSourceReader
planInputPartitions in BasicMicroBatchDataSourceReader returns the list of InputPartitions. This is implemented by SimpleDataSourceReaderFactory
@todo change the number of partitions to 1

SimpleInputPartitionReader listens to Solace and returns the records






