package com.solace.spark.streaming.basic;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.ContinuousInputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;

public class BasicContinuousInputPartitionReaderFactory implements ContinuousInputPartition<InternalRow> {
	private int start;
	private int end;

	public BasicContinuousInputPartitionReaderFactory(int start, int end) {
		this.start = start;
		this.end = end;
	}

	@Override
	public InputPartitionReader<InternalRow> createPartitionReader() {
		return (new BasicContinuousInputPartitionReader(this.start, this.end));
	}

	@Override
	public InputPartitionReader<InternalRow> createContinuousReader(PartitionOffset offset) {
		return new BasicContinuousInputPartitionReader(offset);

	}

}
