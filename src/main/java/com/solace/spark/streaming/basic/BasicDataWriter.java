package com.solace.spark.streaming.basic;

import java.io.IOException;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicDataWriter implements DataWriter {

	@Override
	public void write(Object record) throws IOException {
		log.info("write" + record);
		log.info("record" + record.getClass().toString());
		UnsafeRow ur = (UnsafeRow)record;
		log.info(ur.toString());
		log.info("numfields" + ur.numFields());
		log.info("0 STRING:" + ur.getString(0));
		log.info("1 STRING:" + ur.getString(1));
		Object obj = ur.getBaseObject();
		log.info(obj.getClass().toString() + ":" + obj);
	}

	@Override
	public WriterCommitMessage commit() throws IOException {
		log.info("commit");
		return null;
	}

	@Override
	public void abort() throws IOException {
		log.info("abort");
		
	}

}
