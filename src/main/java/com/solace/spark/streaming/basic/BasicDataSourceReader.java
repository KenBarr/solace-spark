package com.solace.spark.streaming.basic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicDataSourceReader implements DataSourceReader {

	private String hosturi;
	private String vpn;
	private String username;
	private String password;
	private String topic;
	
	public BasicDataSourceReader(DataSourceOptions options) {
		Map<String,String> map = options.asMap();
		hosturi = map.get("hosturi");
		vpn = map.get("vpn");
		username = map.get("username");
		password = map.get("password");
		topic = map.get("topic");
		
		if (hosturi == null || vpn == null || username == null || password == null || topic == null) {
			throw new RuntimeException("option missing: hosturi, vpn, username, password, topic");
		}
		log.info(hosturi + topic);
		
	}

	@Override
	public StructType readSchema() {
		log.info("readSchema");
		
		StructType st = new StructType();
		st = st.add("a", DataTypes.StringType);
		st = st.add("b", DataTypes.IntegerType);
		
		log.info("st:" + st.toString());
		return(st);

		
//		StructType schema = new StructType(new StructField[]{
//			      new StructField("label", DataTypes.StringType, false, Metadata.empty()),
//			      new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
//			    });
//		return(schema);
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

}



