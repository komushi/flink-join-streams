
```
mvn package -Dflink.version=1.13.2
```

```
fields @timestamp, @message
| filter @message like /watermarkedStream/
| parse message /sourceType=(?<sourceType>.*?), eventTime=(?<eventTime>.*?), vin=(?<vin>.*?), source=(?<source>.*?), /
| sort @timestamp asc
| limit 2000
| display @timestamp, eventTime, vin, source, sourceType
```


```
fields @timestamp, @message
| filter @message like /windowed_value/
| parse message /sourceType=(?<sourceType>.*?), windowStart=(?<windowStart>.*?), eventTime=(?<eventTime>.*?), cnt=(?<cnt>.*?), vin=(?<vin>.*?), source=(?<source>.*?), /
| sort @timestamp asc
| limit 2000
| display windowStart, @timestamp, eventTime, cnt, vin, source, sourceType
```


```
fields @timestamp, @message
| filter @message like /ProcessWindowFunction/
| parse message /sourceType=(?<sourceType>.*?), windowStart=(?<windowStart>.*?), eventTime=(?<eventTime>.*?), cnt=(?<cnt>.*?), vin=(?<vin>.*?), source=(?<source>.*?), /
| sort @timestamp asc
| limit 2000
| display windowStart, @timestamp, eventTime, cnt, vin, source, sourceType
```

