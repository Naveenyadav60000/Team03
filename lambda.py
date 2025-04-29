import boto3
import json
import joblib
import os
import pandas as pd
from io import BytesIO

s3 = boto3.client('s3')
bucket = 'fraud-detection-pipeline'
model_key = 'model/fraud_model.pkl'

# Load model once
model_obj = s3.get_object(Bucket=bucket, Key=model_key)
model = joblib.load(model_obj['Body'])

def lambda_handler(event, context):
    for record in event['Records']:
        payload = json.loads(record['kinesis']['data'])
        
        df = pd.DataFrame([payload])
        prediction = model.predict(df)[0]

        # Save result to S3
        result = {
            "input": payload,
            "fraud": int(prediction)
        }
        result_key = f"realtime_predictions/pred_{context.aws_request_id}.json"
        s3.put_object(Bucket=bucket, Key=result_key, Body=json.dumps(result))

    return {
        'statusCode': 200,
        'body': json.dumps('✅ Prediction done')
    }
