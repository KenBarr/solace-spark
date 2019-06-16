package com.solace.spark.streaming.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousInputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import lombok.extern.slf4j.Slf4j;



@Slf4j
public class BasicMicroBatchInputPartitionReader implements MicroBatchReader {
	private BasicOffset start;
	private BasicOffset end;

	
	
	@Override
	public StructType readSchema() {
		log.info("readSchema");
		
		StructType st = new StructType();
		st = st.add("a", DataTypes.StringType);
		st = st.add("b", DataTypes.IntegerType);
		
		log.info("st:" + st.toString());
		return(st);
	}

	@Override
	public List<InputPartition<InternalRow>> planInputPartitions() {
		log.info("planInputPartitions");		
		List<InputPartition<InternalRow>> factoryList = new ArrayList<InputPartition<InternalRow>>();
		factoryList.add(new BasicInputPartitionReaderFactory(0,3));
		factoryList.add(new BasicInputPartitionReaderFactory(4,7));
		log.info("FactoryList:" + factoryList.toString());
		return(factoryList);
	}

	@Override
	public void stop() {
		log.info("stop");		
		
	}

	@Override
	public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
		System.out.println("setOffsetRange:" + start + "," + end);	
		
//		this.start = start.isPresent()?start.get():null;
//		this.end = end.isPresent()?end.get():null;
		this.start = new BasicOffset(0, 1);
		this.end = new BasicOffset(3, 3);

		
	}

	@Override
	public Offset getStartOffset() {
		log.info("getStartOffset:" + this.start);		
		return(this.start);
	}

	@Override
	public Offset getEndOffset() {
		log.info("getEndOffset:" + this.end);		
		return(this.end);
	}


	@Override
	public Offset deserializeOffset(String json) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void commit(Offset end) {
		log.info("commit");		
		
	}

}
