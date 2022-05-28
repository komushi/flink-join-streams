package com.amazonaws.kinesisanalytics.flink.streaming.etl.sim;

import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Arrays;
import java.util.Random;

/** A simple in-memory source. */
public class IisSource implements SourceFunction<Row> {

    private static final long serialVersionUID = 1L;
    private Integer[] speeds;
    private Double[] distances;
    private int intervalInMs;
    private int delayInMs;
    private long crtTime;
    private Boolean isFastfoward;

    private Random rand = new Random();

    private volatile boolean isRunning = true;

    private IisSource(int numOfCars, int interval, int flashback, int delay, Boolean fastfoward) {
        intervalInMs = interval;
        delayInMs = delay;
        crtTime = System.currentTimeMillis() - flashback - 1;
        isFastfoward = fastfoward;
        speeds = new Integer[numOfCars];
        distances = new Double[numOfCars];
        Arrays.fill(speeds, 50);
        Arrays.fill(distances, 0d);
    }

    public static IisSource create(int cars, int interval, int flashback, int delay, Boolean fastfoward) {
        return new IisSource(cars, interval, flashback, delay, fastfoward);
    }

    @Override
    public void run(SourceFunction.SourceContext<Row> ctx)
            throws Exception {

        Thread.sleep(delayInMs);

        while (isRunning) {

            if (!isFastfoward) {
                Thread.sleep(intervalInMs);    
            } else {
                Thread.sleep(10);
            }

            crtTime = crtTime + intervalInMs;
            
            for (int carId = 0; carId < speeds.length; carId++) {
                if (rand.nextBoolean()) {
                    speeds[carId] = Math.min(100, speeds[carId] + 5);
                } else {
                    speeds[carId] = Math.max(0, speeds[carId] - 5);
                }
                distances[carId] += speeds[carId] / 3.6d;

                Row row = Row.withNames();
                
                row.setField("vin", "v" + carId);
                row.setField("speed", speeds[carId]);
                row.setField("distance", distances[carId]);
                row.setField("eventTime", crtTime);
                row.setField("sourceType", 2);
                row.setField("source", "IIS");
                row.setField("attr21", 1);

                ctx.collectWithTimestamp(row, crtTime);
            }

            ctx.emitWatermark(new Watermark(crtTime + 1));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
