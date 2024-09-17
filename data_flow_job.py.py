#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import ReadFromPubSub
import json

# Define Dataflow pipeline options
options = PipelineOptions(
    project="heatex_fanguard",
    runner="DataflowRunner",
    temp_location="gs://SensorData/temp",
    region="us-central1"
)
options.view_as(StandardOptions).streaming = True

# Define schema for BigQuery table
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
        {"name": "anomaly_flag", "type": "BOOLEAN"}
    ]
}

# Transformation function for data processing
def transform_data(record):
    data = json.loads(record.decode('utf-8'))
    data['temperature_fahrenheit'] = (data['temperature'] * 9/5) + 32
    data['rolling_avg_temperature'] = data['temperature']
    data['anomaly_flag'] = data['temperature'] > 80
    return data

# Define the Dataflow pipeline
def run():
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(subscription="projects/heatex_fanguard/subscriptions/sensor-data-subscription")
            | "TransformData" >> beam.Map(transform_data)
            | "WriteToBigQuery" >> WriteToBigQuery(
                "heatex_fanguard:sensor_data_analysis.processed_sensor_data",
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run()

