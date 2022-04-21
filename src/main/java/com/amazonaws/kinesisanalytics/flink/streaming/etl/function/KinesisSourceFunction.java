package com.amazonaws.kinesisanalytics.flink.streaming.etl.function;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;


import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_CONSUMER_NAME;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;

// import software.amazon.kinesis.connectors.flink.FlinkKinesisConsumer;
// import software.amazon.kinesis.connectors.flink.serialization.KinesisDeserializationSchemaWrapper;
// import static software.amazon.kinesis.connectors.flink.config.ConsumerConfigConstants.EFO_CONSUMER_NAME;
// import static software.amazon.kinesis.connectors.flink.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Properties;

import org.apache.flink.formats.json.JsonNodeDeserializationSchema;

public class KinesisSourceFunction {

	public static SourceFunction<ObjectNode> getKinesisSource(ParameterTool parameter, String region) {
		String streamName = parameter.getRequired("INPUT_KDS_STREAM");
		String initialPosition = parameter.get("KDS_INITIAL_POSITION", ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION);

		Properties kinesisConsumerConfig = new Properties();

		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, region);
		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, initialPosition);

		kinesisConsumerConfig.putIfAbsent(RECORD_PUBLISHER_TYPE, EFO.toString());
		kinesisConsumerConfig.putIfAbsent(EFO_CONSUMER_NAME, "basic-efo-flink-app");
		
		return new FlinkKinesisConsumer<>(streamName,
										  new JsonNodeDeserializationSchema(),
                                          kinesisConsumerConfig);
	}
}
