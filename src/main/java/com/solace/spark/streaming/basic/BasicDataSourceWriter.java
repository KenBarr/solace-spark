package com.solace.spark.streaming.basic;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicDataSourceWriter implements DataSourceWriter {

	@Override
	public DataWriterFactory<InternalRow> createWriterFactory() {
		log.info("createWriterFactory");

		return new BasicDataWriterFactory();
	}

	@Override
	public void commit(WriterCommitMessage[] messages) {
		log.info("commit:" + messages );
		
	}

	@Override
	public void abort(WriterCommitMessage[] messages) {
		log.info("abort");
		
	}

}
