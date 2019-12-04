#! /usr/local/bin/python3
import boto3
import sys
 
num=0
TableName = sys.argv[1]
region_name = sys.argv[2]
 
client = boto3.client('dynamodbstreams', region_name=region_name)
 
response = client.list_streams(
    TableName=TableName,
    )
 
if len(response['Streams']) == 0:
    print("DynamoDBStreams is not enabled for", TableName, "table. Please enable it and rerun this script again")
    exit()
 
StreamArn=(response['Streams'][0]['StreamArn'])
 
response = client.describe_stream(
    StreamArn=StreamArn,
    )
 
for shard in response['StreamDescription']['Shards']:
    if 'EndingSequenceNumber' not in shard['SequenceNumberRange']:
        num += 1
 
print("Number of partitions for", TableName, "DDB Table from",region_name, "region is", num)
