package com.amazonaws.kinesisanalytics.flink.streaming.etl.sim;

import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Arrays;
import java.util.Random;

/** A simple in-memory source. */
public class CarSource implements SourceFunction<Row> {

    private static final long serialVersionUID = 1L;
    private Integer[] speeds;
    private Double[] distances;
    private int sourceType;

    private Random rand = new Random();

    private volatile boolean isRunning = true;

    private CarSource(int numOfCars, int type) {
        sourceType = type;
        speeds = new Integer[numOfCars];
        distances = new Double[numOfCars];
        Arrays.fill(speeds, 50);
        Arrays.fill(distances, 0d);
    }

    public static CarSource create(int cars, int type, int interval) {
        return new CarSource(cars, type);
    }

    @Override
    public void run(SourceFunction.SourceContext<Row> ctx)
            throws Exception {

        while (isRunning) {

            Thread.sleep(interval);
            
            for (int carId = 0; carId < speeds.length; carId++) {
                if (rand.nextBoolean()) {
                    speeds[carId] = Math.min(100, speeds[carId] + 5);
                } else {
                    speeds[carId] = Math.max(0, speeds[carId] - 5);
                }
                distances[carId] += speeds[carId] / 3.6d;
                
                long crtTime = System.currentTimeMillis();

                Row row = Row.withNames();
                
                row.setField("vin", carId);
                row.setField("speed", speeds[carId]);
                row.setField("distance", distances[carId]);
                row.setField("eventTime", crtTime);
                row.setField("sourceType", sourceType);

                if (sourceType == 1) {
                    row.setField("source", "CAN");
                } else if (sourceType == 2) {
                    row.setField("source", "IIS");
                    row.setField("attr21", 1);
                } else if (sourceType == 3) {
                    row.setField("source", "IIS");
                    row.setField("attr31", 2);
                    row.setField("attr32", 3);
                }

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
