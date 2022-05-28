# Flink sample to demo the streams join/union

## compile
```
mvn package -Dflink.version=1.13.2
```


## check Watermarked Stream(input after union)
```
fields @timestamp, @message
| filter @message like /watermarkedStream/
| parse message /sourceType=(?<sourceType>.*?), eventTime=(?<eventTime>.*?), vin=(?<vin>.*?), source=(?<source>.*?), /
| sort @timestamp asc
| limit 2000
| display @timestamp, eventTime, vin, source, sourceType
```

## check Windowed Stream(output)
```
fields @timestamp, @message
| filter @message like /windowed_value/
| parse message /sourceType=(?<sourceType>.*?), windowStart=(?<windowStart>.*?), eventTime=(?<eventTime>.*?), cnt=(?<cnt>.*?), vin=(?<vin>.*?), source=(?<source>.*?), /
| sort eventTime asc
| limit 1000
| display windowStart, @timestamp, eventTime, cnt, vin, source, sourceType
```

## Debug ProcessWindowFunction 

```
fields @timestamp, @message
| filter @message like /ProcessWindowFunction:/
| parse message /sourceType=(?<sourceType>.*?), windowStart=(?<windowStart>.*?), eventTime=(?<eventTime>.*?), cnt=(?<cnt>.*?), vin=(?<vin>.*?), source=(?<source>.*?), /
| sort @timestamp asc
| limit 2000
| display windowStart, @timestamp, eventTime, cnt, vin, source, sourceType
```

```
fields @timestamp, @message
| filter @message like /ProcessWindowFunctionTreeMap/
| sort @timestamp asc
| limit 2000
| display @timestamp, message
```

## KDA Flink Parameters


|Category         | Property             | Value       | Description                    |
|:----------------|:---------------------|:-------------------|:-------------------------|
| Streaming applications    |Name                  |  join-streams                  |  |
| Application code location | Amazon S3 bucket      | s3://<flink-jar-bucket> |    |
|                           | Path to S3 object     | flink-join-streams-0.0.1.jar | |
| Scaling                   | Parallelism           | 64 |    |
|                           | Parallelism per KPU   | 4 | |
|                           | Automatic scaling     | false | |
| Runtime properties        ||| |
| Group 'FlinkParallelismProperties'| THREAD_LOGGING |1 | |
|                                   | THREAD_WATERMARKING |32 | |
|                                   | THREAD_WINDOWING |64 | |
| Group 'FlinkApplicationProperties'| DELAY_EVENT_TYPE1 | 60000 | In ms (CarSource) |
|                                   | DELAY_EVENT_TYPE2 | 0 | In ms (IisSource) |
|                                   | DELAY_EVENT_TYPE3 | 0 | In ms (SmsSource) |
|                                   | FLASHBACK_LENGTH | 0 | Flashback in ms  |
|                                   | INTERVAL_EVENT_TYPE1 | 500 | Interval of event type 1 (CarSource) |
|                                   | INTERVAL_EVENT_TYPE2 | 500 | Interval of event type 2 (IisSource) |
|                                   | INTERVAL_EVENT_TYPE3 | 500 | Interval of event type 3 (SmsSource) |
|                                   | NUM_OF_CARS | 2 | Number of cars |
|                                   | WATERMARK_DELAY |60100 | Watermark Delay |
|                                   | WATERMARK_DELAY_UOM | MILLIS | Watermark Delay UOM |
|                                   | WINDOW_SIZE | 100 | Window size |
|                                   | WINDOW_SIZE_UOM | MILLISECONDS | Window size UOM |