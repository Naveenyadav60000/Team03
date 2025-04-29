import pandas as pd
from sklearn.linear_model import LogisticRegression
import joblib
import boto3
from io import BytesIO

# S3 bucket details
bucket = 'fraud-detection-pipeline'
train_key = 'raw/creditcard.csv'
model_key = 'model/fraud_model.pkl'

# Load CSV from S3
s3 = boto3.client('s3')
obj = s3.get_object(Bucket=bucket, Key=train_key)
df = pd.read_csv(obj['Body'])

# Preprocess
df = df.drop_duplicates()
df['Amount'] = df['Amount'] / df['Amount'].max()  # Normalize
X = df.drop(['Class', 'Time'], axis=1)
y = df['Class']

# Train model
model = LogisticRegression()
model.fit(X, y)

# Save model to S3
buffer = BytesIO()
joblib.dump(model, buffer)
buffer.seek(0)
s3.put_object(Bucket=bucket, Key=model_key, Body=buffer)

print("✅ Model trained and saved to S3!")
