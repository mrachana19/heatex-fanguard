#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import json
import time
import joblib
import numpy as np
from google.cloud import pubsub_v1, bigquery, aiplatform
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import classification_report
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import ReadFromPubSub

# Set up GCP configuration
project_id = "heatex_fanguard"
region = "us-central1"
gcs_bucket = "SensorData"
pubsub_topic = "sensor-data-topic"
pubsub_subscription = "sensor-data-subscription"
bigquery_dataset = "sensor_data_analysis"
bigquery_table = "processed_sensor_data"
model_display_name = "heatex_rf_model"
model_gb_display_name = "heatex_gb_model"
model_file_path = f"gs://{gcs_bucket}/models/random_forest_model.pkl"
model_gb_file_path = f"gs://{gcs_bucket}/models/gradient_boosting_model.pkl"
endpoint_id = "your_endpoint_id"  # Replace with actual endpoint ID after deployment

# Initialize GCP clients
pubsub_publisher = pubsub_v1.PublisherClient()
pubsub_topic_path = pubsub_publisher.topic_path(project_id, pubsub_topic)
bigquery_client = bigquery.Client(project=project_id)

# Function to publish data to Pub/Sub
def publish_sensor_data(file_path):
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        message = json.dumps(row.to_dict()).encode("utf-8")
        pubsub_publisher.publish(pubsub_topic_path, data=message)
        print(f"Published message to {pubsub_topic}: {row['sensor_id']}")
        time.sleep(1)  # Simulate real-time data streaming

# Dataflow pipeline for data processing
def run_dataflow():
    options = PipelineOptions(
        project=project_id,
        runner="DataflowRunner",
        temp_location=f"gs://{gcs_bucket}/temp",
        region=region
    )
    options.view_as(StandardOptions).streaming = True

    BQ_SCHEMA = {
        "fields": [
            {"name": "timestamp", "type": "TIMESTAMP"},
            {"name": "sensor_id", "type": "STRING"},
            {"name": "temperature", "type": "FLOAT"},
            {"name": "temperature_fahrenheit", "type": "FLOAT"},
            {"name": "pressure", "type": "FLOAT"},
            {"name": "humidity", "type": "FLOAT"},
            {"name": "velocity", "type": "FLOAT"},
            {"name": "status", "type": "STRING"},
            {"name": "rolling_avg_temperature", "type": "FLOAT"},
            {"name": "anomaly_flag", "type": "BOOLEAN"},
            {"name": "temperature_delta", "type": "FLOAT"}  # New Feature
        ]
    }

    def transform_data(record):
        data = json.loads(record.decode('utf-8'))
        data['temperature_fahrenheit'] = (data['temperature'] * 9/5) + 32
        data['rolling_avg_temperature'] = data['temperature']  # Placeholder for rolling average logic
        data['anomaly_flag'] = data['temperature'] > 80
        data['temperature_delta'] = data['temperature'] - data.get('rolling_avg_temperature', data['temperature'])
        return data

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{pubsub_subscription}")
            | "TransformData" >> beam.Map(transform_data)
            | "WriteToBigQuery" >> WriteToBigQuery(
                f"{project_id}:{bigquery_dataset}.{bigquery_table}",
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

# Function for exploratory data analysis and fetching data from BigQuery
def explore_data():
    query = f"SELECT * FROM `{project_id}.{bigquery_dataset}.{bigquery_table}`"
    df = bigquery_client.query(query).to_dataframe()
    print("Data Overview:\n", df.describe())
    df['temperature'].hist()
    return df

# Function to train and tune multiple models
def train_and_tune_models(df):
    X = df.drop(columns=["status", "timestamp", "sensor_id"])
    y = df["status"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Random Forest model with GridSearchCV for hyperparameter tuning
    rf_model = RandomForestClassifier(random_state=42)
    rf_params = {'n_estimators': [100, 200], 'max_depth': [10, 20]}
    rf_grid = GridSearchCV(rf_model, rf_params, cv=3, scoring='accuracy')
    rf_grid.fit(X_train, y_train)
    print(f"Best Random Forest Params: {rf_grid.best_params_}")
    print("Random Forest Model Evaluation:\n", classification_report(y_test, rf_grid.predict(X_test)))
    joblib.dump(rf_grid.best_estimator_, model_file_path)
    print(f"Random Forest Model saved to {model_file_path}")

    # Gradient Boosting model with GridSearchCV for hyperparameter tuning
    gb_model = GradientBoostingClassifier(random_state=42)
    gb_params = {'n_estimators': [100, 150], 'learning_rate': [0.1, 0.05]}
    gb_grid = GridSearchCV(gb_model, gb_params, cv=3, scoring='accuracy')
    gb_grid.fit(X_train, y_train)
    print(f"Best Gradient Boosting Params: {gb_grid.best_params_}")
    print("Gradient Boosting Model Evaluation:\n", classification_report(y_test, gb_grid.predict(X_test)))
    joblib.dump(gb_grid.best_estimator_, model_gb_file_path)
    print(f"Gradient Boosting Model saved to {model_gb_file_path}")

# Function to deploy models to Vertex AI with versioning
def deploy_model():
    aiplatform.init(project=project_id, location=region)
    
    # Deploy Random Forest Model
    rf_model = aiplatform.Model.upload(
        display_name=f"{model_display_name}_v1",
        artifact_uri=model_file_path,
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.0-23:latest"
    )
    rf_endpoint = rf_model.deploy(machine_type="n1-standard-4")
    print(f"Random Forest Model deployed to endpoint: {rf_endpoint.resource_name}")
    
    # Deploy Gradient Boosting Model
    gb_model = aiplatform.Model.upload(
        display_name=f"{model_gb_display_name}_v1",
        artifact_uri=model_gb_file_path,
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.0-23:latest"
    )
    gb_endpoint = gb_model.deploy(machine_type="n1-standard-4")
    print(f"Gradient Boosting Model deployed to endpoint: {gb_endpoint.resource_name}")

# Function to make predictions and monitor
def predict_and_store():
    query = f"SELECT * FROM `{project_id}.{bigquery_dataset}.new_sensor_data`"
    df = bigquery_client.query(query).to_dataframe()
    endpoint = aiplatform.Endpoint(endpoint_id)
    predictions = endpoint.predict(instances=df.drop(columns=["timestamp", "sensor_id"]).values.tolist())
    df['predictions'] = predictions
    table_id = f"{project_id}.{bigquery_dataset}.predictions"
    bigquery_client.load_table_from_dataframe(df, table_id).result()
    print("Predictions stored in BigQuery.")

# Main function to orchestrate the pipeline
if __name__ == "__main__":
    publish_sensor_data('synthetic_iot_sensor_data_large.csv')
    run_dataflow()
    df = explore_data()
    train_and_tune_models(df)
    deploy_model()
    predict_and_store()

