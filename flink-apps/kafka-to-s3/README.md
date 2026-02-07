# Flink Kafka to S3

Streams data from Kafka to S3 with date-based partitioning.

## Quick Start

1. **Configure** - Copy and edit the properties file:
```bash
cp src/main/resources/flink-application-properties-dev.json \
   src/main/resources/flink-application-properties-local.json
```

Edit with your Kafka and S3 settings.

2. **Run locally**:
```bash
mvn clean package -Plocal
java -jar target/flink-kafka-s3-1.0.jar
```

3. **Build for AWS**:
```bash
mvn clean package
# Upload target/flink-kafka-s3-1.0.jar to S3
```

## Configuration

Edit `flink-application-properties-local.json`:

```json
{
  "PropertyGroupId": "KafkaSource",
  "PropertyMap": {
    "bootstrap.servers": "your-kafka:9092",
    "topic": "your-topic",
    "group.id": "your-group"
  }
}
```

## Features

- SASL_SSL authentication support
- Date-based S3 partitioning (`date=2026-02-07/`)
- Configurable rolling policies
- JSON output format

## AWS Deployment

1. Upload JAR to S3
2. Create Managed Service for Apache Flink application
3. Configure runtime properties (same structure as local config)
4. Start application
