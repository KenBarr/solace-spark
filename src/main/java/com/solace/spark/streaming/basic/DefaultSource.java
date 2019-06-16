package com.solace.spark.streaming.basic;

import java.util.Optional;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultSource implements DataSourceV2, ReadSupport, WriteSupport, ContinuousReadSupport, MicroBatchReadSupport{

	@Override
	public DataSourceReader createReader(DataSourceOptions options) {
		return new BasicDataSourceReader(options);
	}

	@Override
	public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema, SaveMode mode,
			DataSourceOptions options) {
		
		log.info("writeUUID: "+ writeUUID + "StructType:" + schema + " SaveMode: " + mode + "Options: " + options.toString());

		
		return  Optional.of(new BasicDataSourceWriter());
	}

	@Override
	public ContinuousReader createContinuousReader(Optional<StructType> schema, String checkpointLocation,
			DataSourceOptions options) {
		log.info("createContinuousReader");
		return new BasicContinuousDataSourceReader(schema, checkpointLocation, options);

	}

	@Override
	public MicroBatchReader createMicroBatchReader(Optional<StructType> schema, String checkpointLocation,
			DataSourceOptions options) {
		log.info("createMicroBatchReader");
		return new BasicMicroBatchDataSourceReader(schema, checkpointLocation, options);
	}


}
