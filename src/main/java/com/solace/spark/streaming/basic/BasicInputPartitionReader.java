package com.solace.spark.streaming.basic;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import foo.ScalaClass;
import lombok.extern.slf4j.Slf4j;


//class MyObj{
//	private String name;
//	MyObj(String name){
//		this.name = name;
//	}
//}

@Slf4j
public class BasicInputPartitionReader implements InputPartitionReader<InternalRow> {
	
	private int start = -1;
	private int end = -1;
	private int index = 0;

	
	public BasicInputPartitionReader(int start, int end) {
		this.start = start;
		this.end = end;
		this.index = start;
	}
	
//	private String values[] = {"1","2","3","4","5"};
//	private MyObj values[] = {
//			new MyObj("1"),
//			new MyObj("2"),
//			new MyObj("3"),
//			new MyObj("4"),
//			new MyObj("5"),
//			};

	@Override
	public void close() throws IOException {
		log.info("close");

	}

	@Override
	public boolean next() throws IOException {
		return (index <= this.end);
	}

	@Override
	public InternalRow get() {
		
		
		String aString = "aString";
		int anInteger = 2;
		ScalaClass sc = new ScalaClass(aString, anInteger);
		InternalRow ir = sc.toInternalRow("" + index, "" + index);
		
		log.info("InternalRow:" + "(" + start + "," + end + ") " + ir);
		index++;
		return(ir);
		
//		return(null);
	}


}
