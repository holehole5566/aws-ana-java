# Kinesis to S3 Flink App

Streams data from AWS Kinesis to S3 with date partitioning and configurable starting position.

## Features

- Timestamp-based starting position
- Date-partitioned S3 output
- Configurable rollover policies

## Configuration

Edit `src/main/resources/flink-application-properties-local.json`:

```json
{
  "KinesisSource": {
    "stream.name": "test0310",
    "aws.region": "us-east-1",
    "start.position": "AT_TIMESTAMP",  // or "LATEST", "TRIM_HORIZON"
    "start.timestamp": "2026-03-10T00:00:00Z"
  },
  "S3Sink": {
    "output.path": "s3://flink-sink-poyenc/test0310/"
  }
}
```

## Build & Run

```bash
mvn clean package -Plocal
java -jar target/flink-kinesis-s3-1.0.jar
```

## Deploy to AWS

```bash
mvn clean package
aws s3 cp target/flink-kinesis-s3-1.0.jar s3://your-bucket/
```
