# Flink Applications

Collection of Apache Flink streaming applications for AWS.

## Projects

- **kafka-to-s3**: Kafka → S3 with date partitioning
- **msk-to-iceberg**: MSK → Iceberg data lake
- **mysql-to-elasticsearch**: MySQL CDC → Elasticsearch

## Quick Start

Each project follows the same pattern:

1. **Configure**: Copy `-dev.json` to `-local.json` and add your credentials
2. **Build**: `mvn clean package -Plocal`
3. **Run**: `java -jar target/[project]-1.0.jar`

## Configuration

All apps use JSON property files in `src/main/resources/`:
- `flink-application-properties-dev.json` - Template (commit to git)
- `flink-application-properties-local.json` - Your credentials (gitignored)

## AWS Deployment

```bash
mvn clean package
aws s3 cp target/[project]-1.0.jar s3://your-bucket/
```

Configure runtime properties in AWS Console matching your local config structure.
