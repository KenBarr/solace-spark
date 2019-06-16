package com.solace.spark.streaming.basic;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicInputPartitionReaderFactory implements InputPartition<InternalRow> {
	private int start;
	private int end;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public BasicInputPartitionReaderFactory(int start, int end) {
		this.start = start;
		this.end = end;
	}

	@Override
	public InputPartitionReader<InternalRow> createPartitionReader() {
		return (new BasicInputPartitionReader(this.start, this.end));
	}

}
