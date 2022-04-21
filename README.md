
```
mvn package -Dflink.version=1.13.2
```

```
aws s3 cp ./target/flink-kinesis-benchmark-0.0.1.jar s3://flink-jar-bucket/

aws s3 cp ./target/flink-kinesis-benchmark-0.0.1.jar s3://cdf-daihatsu-temp/

```