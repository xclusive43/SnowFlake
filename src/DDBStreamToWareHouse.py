from datetime import datetime
import pandas as pd
import boto3
from io import StringIO

def handle_insert(record):
    print("Handling Insert: ", record)
    record_dict = {}

    for key, value in record['dynamodb']['NewImage'].items():
        for dt, col in value.items():
            record_dict[key] = col  # Ensure correct handling of DynamoDB types

    dff = pd.DataFrame([record_dict])
    return dff


def lambda_handler(event, context):
    print(event)
    df = pd.DataFrame()

    for record in event['Records']:
        table = record['eventSourceARN'].split("/")[1]

        if record['eventName'] == "INSERT": 
            dff = handle_insert(record)
            if df.empty:
                df = dff
            else:
                df = pd.concat([df, dff], ignore_index=True)

    if not df.empty:
        all_columns = list(df)
        df[all_columns] = df[all_columns].astype(str)

        timestamp = str(datetime.now())
        path = table + "_" + timestamp + ".csv"
        print(event)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)

        s3 = boto3.client('s3')
        bucketName = "de-project-datewithdata2"
        key = "snowflake/" + table + "_" + timestamp + ".csv"
        print(key)
        
        s3.put_object(Bucket=bucketName, Key=key, Body=csv_buffer.getvalue(),)

    print('Successfully processed %s records.' % str(len(event['Records'])))
