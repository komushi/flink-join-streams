
```
mvn package -Dflink.version=1.13.2
```

```
aws s3 cp ./target/flink-kinesis-benchmark-0.0.1.jar s3://flink-jar-bucket/

aws s3 cp ./target/flink-kinesis-benchmark-0.0.1.jar s3://cdf-daihatsu-temp/

```


```
fields @timestamp, @message
| filter @message like /ProcessWindowFunction/
| parse message /sourceType=(?<sourceType>.*?), windowStart=(?<windowStart>.*?), eventTime=(?<eventTime>.*?), cnt=(?<cnt>.*?), vin=(?<vin>.*?), source=(?<source>.*?), /
| sort windowStart asc
| limit 100
| display @timestamp, eventTime, cnt, vin, source, sourceType, windowStart
```


```
fields @timestamp, @message
| filter @message like /windowed_value/
| parse message /sourceType=(?<sourceType>.*?), windowStart=(?<windowStart>.*?), eventTime=(?<eventTime>.*?), cnt=(?<cnt>.*?), vin=(?<vin>.*?), source=(?<source>.*?), /
| sort windowStart asc
| limit 100
| display @timestamp, eventTime, cnt, vin, source, sourceType, windowStart
```