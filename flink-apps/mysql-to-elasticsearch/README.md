# Flink MySQL CDC to Elasticsearch 專案

## 編譯指令

### 本地開發環境編譯 (包含所有依賴)
```bash
mvn clean package -Plocal
```

### AWS環境編譯 (Flink core為provided)
```bash
mvn clean package -Paws
```
或者 (因為aws是默認profile)
```bash
mvn clean package
```

## 運行指令

### 本地運行
```bash
java -cp target/flink-mysql-elasticsearch-1.0.jar org.example.MysqlToElasticsearchJob
```

### AWS Managed Flink 部署

#### 1. 上傳 JAR 到 S3
```bash
# 編譯完成後上傳到 S3
aws s3 cp target/flink-mysql-elasticsearch-1.0.jar s3://flink-code-poyenc/
```

#### 2. 在 AWS Managed Flink 中配置
- Application JAR location: s3://flink-code-poyenc/flink-mysql-elasticsearch-1.0.jar
- Main class: org.example.MysqlToElasticsearchJob
- Parallelism: 4 (已在代碼中設置)

## 注意事項

1. 請確保在運行前填入正確的密碼：
   - MySQL密碼：在 MysqlToElasticsearchJob.java 中的 'password' 參數
   - Elasticsearch密碼：在 MysqlToElasticsearchJob.java 中的 'password' 參數

2. 確保網路連接：
   - Flink 能夠連接到 MySQL RDS
   - Flink 能夠連接到 Elasticsearch VPC endpoint

3. MySQL CDC 配置：
   - 確保 MySQL 開啟了 binlog
   - 確保用戶有足夠的權限讀取 binlog