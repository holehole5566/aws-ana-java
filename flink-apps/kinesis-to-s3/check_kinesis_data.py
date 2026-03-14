#!/usr/bin/env python3
import boto3
import json
import base64
from datetime import datetime

stream_name = "test0310"
region = "us-east-1"

client = boto3.client('kinesis', region_name=region)

print(f"Consuming from stream: {stream_name}")
print("=" * 60)

# Get shard iterator
response = client.describe_stream(StreamName=stream_name)
shard_id = response['StreamDescription']['Shards'][0]['ShardId']

shard_iterator = client.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType='TRIM_HORIZON'
)['ShardIterator']

print(f"Waiting for records from shard: {shard_id}\n")

count = 0
import time
while count < 10:  # Get first 10 records
    response = client.get_records(ShardIterator=shard_iterator, Limit=10)
    
    for record in response['Records']:
        count += 1
        data = record['Data'].decode('utf-8')
        
        print(f"Record #{count}")
        print(f"Timestamp: {record['ApproximateArrivalTimestamp']}")
        print(f"Partition Key: {record['PartitionKey']}")
        print(f"Raw Data: {data}")
        
        # Try to parse as JSON
        try:
            parsed = json.loads(data)
            print(f"Parsed JSON: {json.dumps(parsed, indent=2)}")
        except:
            print("(Not JSON format)")
        
        print("-" * 60)
        
        if count >= 10:
            break
    
    if 'NextShardIterator' not in response:
        print("Stream closed or no more data")
        break
        
    shard_iterator = response['NextShardIterator']
    
    if not response['Records']:
        print("No records yet, waiting...")
        time.sleep(2)

print(f"\nTotal records consumed: {count}")
