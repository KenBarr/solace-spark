package com.solace.spark.streaming.basic;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicOffset extends Offset{
	private int val;
	private int partitions;
	@Override
	public String json() {
//		log.info("json");
		return "[]";
		
	}
	
	BasicOffset(int val, int parititions){
		super();
		this.val = val;
		this.partitions = parititions;
		
	}
	
	public String toString() {
//		log.info("toString");

		return "BasicOffset[val:" + this.val + ",partitions:" + this.partitions + "]"; 
	}
	
	public boolean equals(BasicOffset off) {
		log.info("equals");

		return false;
		
	}
	
}
