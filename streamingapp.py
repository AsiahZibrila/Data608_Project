import boto3
import pandas as pd
from io import StringIO, BytesIO
import random
import time
import streamlit as st

# Step 1: Load CSV Data from S3
bucket_name = "practice608"
file_key = 'data_practice608/superstore_data.csv'
save_key = 'practice608/streamed.csv'

s3_client = boto3.client('s3')
try:
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print("Successfully retrieved the file from S3")
        csv_content = response['Body'].read().decode('utf-8')
        data = pd.read_csv(StringIO(csv_content))
    else:
        print(f"Failed to retrieve the file from S3. Status - {status}")
        data = pd.DataFrame()  # create an empty DataFrame if loading fails
except s3_client.exceptions.NoSuchKey:
    print(f"The specified key does not exist: {file_key}")
    data = pd.DataFrame()  # create an empty DataFrame if loading fails

# Step 2: Simulate Real-Time Data Generation
def get_random_row(data):
    return data.sample(1).to_dict(orient='records')[0]

def simulate_data_stream(data, interval=1):
    while True:
        row = get_random_row(data)
        yield row
        time.sleep(interval)

# Function to upload data to S3
def upload_to_s3(data, bucket_name, save_key):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=save_key, Body=csv_buffer.getvalue())
    print(f"Data uploaded to {save_key}")

# Step 3: Stream Data with Streamlit
st.title("Real-Time E-commerce Customer Shopping Simulation")

# Create a placeholder for the data
data_placeholder = st.empty()

# List to store streamed data
streamed_data = []

# Simulate streaming data
if not data.empty:
    data_stream = simulate_data_stream(data, interval=2)

    # Display streaming data
    for row in data_stream:
        data_placeholder.write(row)
        streamed_data.append(row)
        
        # Convert list of dictionaries to DataFrame
        streamed_df = pd.DataFrame(streamed_data)

        # Save streamed data to S3
        upload_to_s3(streamed_df, bucket_name, save_key)
else:
    st.write("No data available to stream.")


#RUN TO SHOW STREMLIT APP : streamlit run streamingapp.py