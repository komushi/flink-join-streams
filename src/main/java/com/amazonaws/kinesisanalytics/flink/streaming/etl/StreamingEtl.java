package com.amazonaws.kinesisanalytics.flink.streaming.etl;


// import com.amazonaws.kinesisanalytics.flink.streaming.etl.function.KinesisSinkFunction;
import com.amazonaws.kinesisanalytics.flink.streaming.etl.function.KinesisSourceFunction;

import com.amazonaws.kinesisanalytics.flink.streaming.etl.utils.ParameterToolUtils;
import com.amazonaws.kinesisanalytics.flink.streaming.etl.utils.FormatUtils;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.kinesis.shaded.com.amazonaws.regions.Regions;

import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.types.Row;

import com.amazonaws.kinesisanalytics.flink.streaming.etl.sim.CarSource;
import com.amazonaws.kinesisanalytics.flink.streaming.etl.sim.SmsSource;
import com.amazonaws.kinesisanalytics.flink.streaming.etl.sim.IisSource;

import java.util.concurrent.TimeUnit;
import java.time.temporal.ChronoUnit;

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
        long WINDOW_SIZE = Long.parseLong(parameter.get("WINDOW_SIZE"));
        String WINDOW_SIZE_UOM = parameter.get("WINDOW_SIZE_UOM");
        int NUM_OF_CARS = Integer.parseInt(parameter.get("NUM_OF_CARS"));
		long WATERMARK_DELAY = Long.parseLong(parameter.get("WATERMARK_DELAY"));
		String WATERMARK_DELAY_UOM = parameter.get("WATERMARK_DELAY_UOM");
		int INTERVAL_EVENT_TYPE1 = Integer.parseInt(parameter.get("INTERVAL_EVENT_TYPE1"));
		int INTERVAL_EVENT_TYPE2 = Integer.parseInt(parameter.get("INTERVAL_EVENT_TYPE2"));
		int INTERVAL_EVENT_TYPE3 = Integer.parseInt(parameter.get("INTERVAL_EVENT_TYPE3"));

		int THREAD_WATERMARKING = Integer.parseInt(parameter.get("THREAD_WATERMARKING"));
		int THREAD_WINDOWING = Integer.parseInt(parameter.get("THREAD_WINDOWING"));
		int THREAD_LOGGING1 = Integer.parseInt(parameter.get("THREAD_LOGGING1"));
		int THREAD_LOGGING2 = Integer.parseInt(parameter.get("THREAD_LOGGING2"));


		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Row> events_first = null;
		DataStream<Row> events_second = null;
		DataStream<Row> events_third = null;

		// check stream name
		if (parameter.has("INPUT_KDS_STREAM_FIRST") && parameter.has("INPUT_KDS_STREAM_SECOND") && parameter.has("INPUT_KDS_STREAM_THRID")) {
			// events_first = env.addSource(KinesisSourceFunction.getKinesisSource(parameter, DEFAULT_REGION_NAME))
			// 	.setParallelism(32)
			// 	.name("Kinesis source");
		} else {
			events_first = env.addSource(CarSource.create(NUM_OF_CARS, INTERVAL_EVENT_TYPE1)).name("events_first");
			events_second = env.addSource(IisSource.create(NUM_OF_CARS, INTERVAL_EVENT_TYPE2)).name("events_second");
			events_third = env.addSource(SmsSource.create(NUM_OF_CARS, INTERVAL_EVENT_TYPE3)).name("events_third");
		}

		DataStream<Row> inputStream = (events_first.union(events_second)).union(events_third);

		WatermarkStrategy<Row> watermarkStrategy = WatermarkStrategy
	        .<Row>forBoundedOutOfOrderness(Duration.of(WATERMARK_DELAY, ChronoUnit.valueOf(WATERMARK_DELAY_UOM)))
	        .withTimestampAssigner((event, timestamp) -> {
	        	return Long.parseLong(String.valueOf(event.getField("eventTime")));
	        });

		DataStream<Row> watermarkedStream = inputStream
			.assignTimestampsAndWatermarks(watermarkStrategy)
			.setParallelism(THREAD_WATERMARKING)
	        .name("Watermarking");

		watermarkedStream.map(event -> {
				LOG.warn("watermarkedStream: " + event.toString());
				return event;
			})
			.setParallelism(THREAD_LOGGING1)
	        .name("Logging1");


		DataStream<Row> resultStream = watermarkedStream
            // .keyBy(new KeySelector<Row, String>() {
            //     @Override
            //     public String getKey(Row record) throws Exception {
            //     	LOG.warn("keyBy: " + record.toString());
            //         return String.valueOf(record.getField("vin"));
            //     }
            // })
            .keyBy(new KeySelector<Row, String>() {
                @Override
                public String getKey(Row record) throws Exception {
                	LOG.warn("keyBy: " + record.toString());
                    return record.getField("vin").toString();
                }
            })
            // .keyBy(record -> record.getField("vin").toString())
            .window(TumblingEventTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.valueOf(WINDOW_SIZE_UOM))))
            .process(new ProcessTumblingWindowFunction())
            .setParallelism(THREAD_WINDOWING)
            .name("Window");

        resultStream.map((Row event) -> {
                LOG.warn("windowed_value: " + event.toString());
                return event;
            })
        	.setParallelism(THREAD_LOGGING2)
        	.name("Logging2");


        resultStream.addSink(new DiscardingSink<>());


		env.execute();
	}

    private static class ProcessTumblingWindowFunction
            extends ProcessWindowFunction<Row, Row, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Row> input,
                            Collector<Row> out) {

	        // sort the input with createdOn
	        TreeMap<Long, Row> treeMap = new TreeMap<>();
	        
	        input.forEach(event -> {
	            treeMap.put(Long.parseLong(String.valueOf(event.getField("eventTime"))), event);
	        });

	        LOG.warn("ProcessWindowFunctionTreeMap: " + treeMap.size());

	        int cnt = 0;

	        for(SortedMap.Entry<Long, Row> entry : treeMap.entrySet()) {
	            Row event = entry.getValue();
	            cnt++;

	            event.setField("cnt", cnt);
	            event.setField("windowStart", context.window().getStart());

	            LOG.warn("ProcessWindowFunction: " + event.toString());
	            
	            out.collect(event);
	        }
        }
    }
}