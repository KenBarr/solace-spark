package com.solace.spark.streaming.basic;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicContinuousDataSourceReader implements ContinuousReader {
	
	
	public BasicContinuousDataSourceReader(Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
		log.info("BasicDataSourceContinuousReader:" + schema + "," + checkpointLocation + "," + options);
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

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
		factoryList.add(new BasicContinuousInputPartitionReaderFactory(0,3));
		log.info("FactoryList:" + factoryList.toString());
		return(factoryList);
	}

	@Override
	public Offset mergeOffsets(PartitionOffset[] offsets) {
		log.info("mergeOffsets:" + offsets);		
		return null;
	}

	@Override
	public Offset deserializeOffset(String json) {
		log.info("deserializeOffset:" + json);		
		return null;
	}

	@Override
	public void setStartOffset(Optional<Offset> start) {
		log.info("setStartOffset:" + start);		

	}

	@Override
	public Offset getStartOffset() {
		log.info("getStartOffset");		

		return null;
	}

	@Override
	public void commit(Offset end) {
		log.info("commit:" + end);		

	}

}
