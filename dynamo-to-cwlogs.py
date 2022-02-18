import time
import boto3
import json

cwlogs = None
ddb = None

def lambda_handler(event, context):
    global cwlogs, ddb
    if not cwlogs and not ddb:
        cwlogs = boto3.client('logs')
        ddb = boto3.client('dynamodb')
    
    table_items = ddb.scan(
        TableName="comments"    
    )
    
    timestamp = int(round(time.time() * 1000))
    
    response = cwlogs.describe_log_streams(logGroupName='database-dump')
    token = response['logStreams'][0]['uploadSequenceToken']
    
    
    response = cwlogs.put_log_events(
        logGroupName="database-dump",
        logStreamName="dynamo",
        logEvents=[
            {
                'timestamp': timestamp,
                'message': json.dumps(table_items)
            }
        ],
        sequenceToken=token
    )
        
    return 'Data Added to CWLogs'
