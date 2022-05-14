package com.amazonaws.kinesisanalytics.flink.streaming.etl.sim;

import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Arrays;
import java.util.Random;

/** A simple in-memory source. */
public class SmsSource implements SourceFunction<Row> {

    private static final long serialVersionUID = 1L;
    private Integer[] speeds;
    private Double[] distances;
    private int intervalInMs;

    private Random rand = new Random();

    private volatile boolean isRunning = true;

    private SmsSource(int numOfCars, int interval) {
        intervalInMs = interval;
        speeds = new Integer[numOfCars];
        distances = new Double[numOfCars];
        Arrays.fill(speeds, 50);
        Arrays.fill(distances, 0d);
    }

    public static SmsSource create(int cars, int interval) {
        return new SmsSource(cars, interval);
    }

    @Override
    public void run(SourceFunction.SourceContext<Row> ctx)
            throws Exception {

        while (isRunning) {

            Thread.sleep(intervalInMs);
            
            for (int carId = 0; carId < speeds.length; carId++) {
                if (rand.nextBoolean()) {
                    speeds[carId] = Math.min(100, speeds[carId] + 5);
                } else {
                    speeds[carId] = Math.max(0, speeds[carId] - 5);
                }
                distances[carId] += speeds[carId] / 3.6d;
                
                long crtTime = System.currentTimeMillis() - 2;

                Row row = Row.withNames();
                
                row.setField("vin", "v" + carId);
                row.setField("speed", speeds[carId]);
                row.setField("distance", distances[carId]);
                row.setField("eventTime", crtTime);
                row.setField("sourceType", 3);
                row.setField("source", "SMS");
                row.setField("attr31", 1);
                row.setField("attr32", 1);

                ctx.collectWithTimestamp(row, crtTime);
            }

            ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
