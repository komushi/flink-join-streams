package com.amazonaws.kinesisanalytics.flink.streaming.etl;


// import com.amazonaws.kinesisanalytics.flink.streaming.etl.function.KinesisSinkFunction;
import com.amazonaws.kinesisanalytics.flink.streaming.etl.function.KinesisSourceFunction;

import com.amazonaws.kinesisanalytics.flink.streaming.etl.utils.ParameterToolUtils;
import com.amazonaws.kinesisanalytics.flink.streaming.etl.utils.FormatUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

// import com.fasterxml.jackson.databind.DeserializationFeature;
// import com.fasterxml.jackson.databind.MapperFeature;
// import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.time.Duration;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.kinesis.shaded.com.amazonaws.regions.Regions;




import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingEtl {
	
	private static final Logger LOG = LoggerFactory.getLogger(StreamingEtl.class);
	
	private static final String DEFAULT_REGION_NAME;
	static {
		String regionName = "ap-northeast-1";

		try {
			regionName = Regions.getCurrentRegion().getName();
		} catch (Exception e) {
		} finally {
			DEFAULT_REGION_NAME = regionName;
		}
	}

	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

		// retrieve parameters
        // int s3UrlStreamThreads = Integer.parseInt(parameter.get("SOURCE_STREAM_THREADS"));
        
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		LOG.warn("******zzzzzzzzz****Parallelism is {}***********", env.getParallelism()); 
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(10);
		// LOG.info("******zzzzzzzzz****tStreamTimeCharacteristic is {}***********", env.getStreamTimeCharacteristic());

		// check stream name
		if ( !parameter.has("INPUT_KDS_STREAM")) {
			throw new RuntimeException("zzz You must specify a single source named INPUT_KDS_STREAM");
		}

		ObjectMapper jsonParser = new ObjectMapper();

		// task1: get stream from KinesisStream 
		DataStream<ObjectNode> events = env.addSource(KinesisSourceFunction.getKinesisSource(parameter, DEFAULT_REGION_NAME))
					    .setParallelism(32)
						.name("Kinesis source");

        DataStream<Tuple4<String, Timestamp, String, Long>> inputStream = events
			.map((ObjectNode object) -> {
				//For debugging input
				LOG.warn(object.toString());

				JsonNode jsonNode = jsonParser.readValue(object.toString(), JsonNode.class);

				Timestamp eventTime = FormatUtils.getTimestamp(jsonNode.get("eventTime").asText());

				return new Tuple4<String, Timestamp, String, Long>(
					jsonNode.get("vin").asText(),
					eventTime,
					jsonNode.get("key").asText(),
					Long.valueOf(jsonNode.get("value").asText().trim())
				);
			}).returns(Types.TUPLE(Types.STRING, Types.SQL_TIMESTAMP, Types.STRING, Types.LONG))
			.setParallelism(64)
			.name("Tuple transform");

		WatermarkStrategy<Tuple4<String, Timestamp, String, Long>> watermarkStrategy = WatermarkStrategy
	        .<Tuple4<String, Timestamp, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
	        .withTimestampAssigner((event, timestamp) -> {
	        	return event.f1.getTime();
	        });

		DataStream<Tuple4<String, Timestamp, String, Long>> watermarkedStream = inputStream
			.assignTimestampsAndWatermarks(watermarkStrategy)
			.setParallelism(30)
	        .name("Watermarking");

        inputStream.addSink(new DiscardingSink<>());

		// Sink bigdata to KDS=>KDF=>S3 
 	// 	if (parameter.has("OUTPUT_KDS_STREAM")) {
	 //        FilterFunction<BigDataAllModel> filterNormal = new FilterFunction<BigDataAllModel>() {
	 //        	private static final long serialVersionUID = 1L;
	 //            @Override
	 //            public boolean filter(BigDataAllModel value) {
	 //                return value.getStatus() == 0;
	 //            }
	 //        };
	        
		// 	decodedStream.filter(filterNormal)
		// 		.addSink(KinesisSinkFunction.getKinesisSink(parameter, DEFAULT_REGION_NAME))
		// 		.setParallelism(printSinkThreads)
		// 		.name("Kinesis sink");
		// }

		
 

		env.execute();
	}

}