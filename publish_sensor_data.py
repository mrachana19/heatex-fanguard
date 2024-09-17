#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from google.cloud import pubsub_v1
import pandas as pd
import time
import json

# Initialize Pub/Sub publisher
project_id = "heatex_fanguard"  # Replace with your actual GCP project ID
topic_id = "sensor-data-topic"  # Single Pub/Sub topic for all sensors
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Load synthetic data
df = pd.read_csv('siot_sensor_data.csv')  # Ensure the file path is correct

# Function to publish messages to Pub/Sub
def publish_data():
    for index, row in df.iterrows():
        message = row.to_dict()  # Convert each row to a dictionary
        message_data = json.dumps(message).encode("utf-8")  # Convert dictionary to JSON and encode as bytes
        future = publisher.publish(topic_path, data=message_data)  # Publish to the single topic
        print(f"Published message ID: {future.result()}")
        time.sleep(1)  # Simulate real-time streaming with a delay

if __name__ == "__main__":
    publish_data()

