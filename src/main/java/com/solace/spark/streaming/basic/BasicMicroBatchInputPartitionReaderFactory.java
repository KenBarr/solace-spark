package com.solace.spark.streaming.basic;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.ContinuousInputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicMicroBatchInputPartitionReaderFactory implements InputPartition<InternalRow> {
	private int start;
	private int end;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public BasicMicroBatchInputPartitionReaderFactory(int start, int end) {
		this.start = start;
		this.end = end;
	}

	@Override
	public InputPartitionReader<InternalRow> createPartitionReader() {
		return (new BasicInputPartitionReader(this.start, this.end));
	}


}
