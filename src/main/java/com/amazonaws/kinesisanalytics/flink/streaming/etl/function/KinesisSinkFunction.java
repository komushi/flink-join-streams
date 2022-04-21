package com.amazonaws.kinesisanalytics.flink.streaming.etl.function;


import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;

import java.util.Properties;
import java.util.UUID;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

public class KinesisSinkFunction {

	public static KinesisStreamsSink<Tuple3> getKinesisSink(ParameterTool parameter, String region) {
		String streamName = parameter.getRequired("OUTPUT_KDS_STREAM");
		
		Properties properties = new Properties();
		properties.setProperty(AWSConfigConstants.AWS_REGION, region);
		properties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");

		KinesisStreamsSink<Tuple3> producer =
		    KinesisStreamsSink.<Tuple3>builder()
		        .setKinesisClientProperties(properties)
		        .setSerializationSchema(new SimpleStringSchema()) // TODO
		        .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
		        .setStreamName(streamName)
		        .setFailOnError(true)
		        .setDefaultPartition("0")
		        .build();

		return producer;
	}    
}
