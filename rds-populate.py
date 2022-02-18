import sys
import logging
import pymysql
import boto3
import base64
import json
import time

secret_name = "rdspassword"

# Create a Secrets Manager client
session = boto3.session.Session()
sm = boto3.client('secretsmanager')

get_secret_value_response = sm.get_secret_value(SecretId=secret_name)

secret = eval(get_secret_value_response['SecretString'])

cwlogs = None

rds_host  = secret['host']
name = secret['username']
password = secret['password']
db_name = secret['dbname']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

def lambda_handler(event, context):
    
    """
    This function fetches content from MySQL RDS instance
    """
    global cwlogs
    if not cwlogs:
        cwlogs = boto3.client('logs')

    item_count = 0
    data = []
    with conn.cursor() as cur:
        try:
            cur.execute("create table Employee ( EmpID  int NOT NULL, Name varchar(255) NOT NULL, PRIMARY KEY (EmpID))")
            cur.execute('insert into Employee (EmpID, Name) values(1, "Joe")')
            cur.execute('insert into Employee (EmpID, Name) values(2, "Bob")')
            cur.execute('insert into Employee (EmpID, Name) values(3, "Mary")')
            conn.commit()
        except:
            print ("Table 'Employee' already exists")
        cur.execute("select * from Employee")
        for row in cur:
            item_count += 1
            logger.info(row)
            print(row)
            data.append(row)
    conn.commit()
    
    timestamp = int(round(time.time() * 1000))
    
    response = cwlogs.describe_log_streams(logGroupName='database-dump')
    token = response['logStreams'][1]['uploadSequenceToken']
    
    response = cwlogs.put_log_events(
        logGroupName="database-dump",
        logStreamName="rds",
        logEvents=[
            {
                'timestamp': timestamp,
                'message': data
            }
        ],
        sequenceToken=token
    )
    
    return "Added %d items from RDS MySQL table" %(item_count)
