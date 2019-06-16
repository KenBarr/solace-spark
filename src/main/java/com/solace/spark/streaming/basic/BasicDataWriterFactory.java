package com.solace.spark.streaming.basic;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;

public class BasicDataWriterFactory implements DataWriterFactory<InternalRow>{

	@Override
	public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
		return new BasicDataWriter();
	}


}
