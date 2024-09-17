#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Load the synthetic data from the generated CSV file
df = pd.read_csv('iot_sensor_data_large.csv', parse_dates=['timestamp'])

# Initial Data Overview
print("Initial Data Info:")
print(df.info())

# Check for missing values
print("\nMissing Values Before Preprocessing:")
print(df.isnull().sum())

# Display initial summary statistics
print("\nSummary Statistics Before Preprocessing:")
print(df.describe(include='all'))

# Set plot style
sns.set(style="darkgrid")

# Before Preprocessing Visualizations

# 1. Distribution of Temperature
plt.figure(figsize=(10, 6))
sns.histplot(df['temperature'], bins=30, kde=True, color='blue')
plt.title('Distribution of Temperature (Before Preprocessing)')
plt.xlabel('Temperature (°C)')
plt.ylabel('Frequency')
plt.show()

# 2. Distribution of Pressure
plt.figure(figsize=(10, 6))
sns.histplot(df['pressure'], bins=30, kde=True, color='green')
plt.title('Distribution of Pressure (Before Preprocessing)')
plt.xlabel('Pressure (Pa)')
plt.ylabel('Frequency')
plt.show()

# 3. Distribution of Humidity
plt.figure(figsize=(10, 6))
sns.histplot(df['humidity'], bins=30, kde=True, color='orange')
plt.title('Distribution of Humidity (Before Preprocessing)')
plt.xlabel('Humidity (%)')
plt.ylabel('Frequency')
plt.show()

# 4. Distribution of Velocity
plt.figure(figsize=(10, 6))
sns.histplot(df['velocity'], bins=30, kde=True, color='red')
plt.title('Distribution of Velocity (Before Preprocessing)')
plt.xlabel('Velocity (m/s)')
plt.ylabel('Frequency')
plt.show()

# 5. Count Plot of Sensor Status
plt.figure(figsize=(8, 6))
sns.countplot(x='status', data=df, palette='viridis')
plt.title('Count of Sensor Status (Before Preprocessing)')
plt.xlabel('Status')
plt.ylabel('Count')
plt.show()

# 6. Correlation Heatmap Before Preprocessing
plt.figure(figsize=(10, 8))
sns.heatmap(df.corr(numeric_only=True), annot=True, cmap='coolwarm', linewidths=0.5)
plt.title('Correlation Heatmap Before Preprocessing')
plt.show()

# Data Cleaning and Preprocessing

# Handling outliers: Clipping temperature, pressure, humidity, and velocity within 1.5 times the IQR
def clip_outliers(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    df[column] = np.clip(df[column], lower_bound, upper_bound)

# Apply outlier clipping
clip_outliers(df, 'temperature')
clip_outliers(df, 'pressure')
clip_outliers(df, 'humidity')
clip_outliers(df, 'velocity')

# Feature Engineering: Adding rolling average for temperature
df['temp_rolling_avg'] = df.groupby('sensor_id')['temperature'].rolling(window=10).mean().reset_index(0, drop=True)
df['temp_rolling_avg'].fillna(df['temperature'], inplace=True)

# After Preprocessing Visualizations

# 1. Distribution of Temperature After Preprocessing
plt.figure(figsize=(10, 6))
sns.histplot(df['temperature'], bins=30, kde=True, color='blue')
plt.title('Distribution of Temperature (After Preprocessing)')
plt.xlabel('Temperature (°C)')
plt.ylabel('Frequency')
plt.show()

# 2. Distribution of Pressure After Preprocessing
plt.figure(figsize=(10, 6))
sns.histplot(df['pressure'], bins=30, kde=True, color='green')
plt.title('Distribution of Pressure (After Preprocessing)')
plt.xlabel('Pressure (Pa)')
plt.ylabel('Frequency')
plt.show()

# 3. Distribution of Humidity After Preprocessing
plt.figure(figsize=(10, 6))
sns.histplot(df['humidity'], bins=30, kde=True, color='orange')
plt.title('Distribution of Humidity (After Preprocessing)')
plt.xlabel('Humidity (%)')
plt.ylabel('Frequency')
plt.show()

# 4. Distribution of Velocity After Preprocessing
plt.figure(figsize=(10, 6))
sns.histplot(df['velocity'], bins=30, kde=True, color='red')
plt.title('Distribution of Velocity (After Preprocessing)')
plt.xlabel('Velocity (m/s)')
plt.ylabel('Frequency')
plt.show()

# 5. Count Plot of Sensor Status After Preprocessing (No change expected)
plt.figure(figsize=(8, 6))
sns.countplot(x='status', data=df, palette='viridis')
plt.title('Count of Sensor Status (After Preprocessing)')
plt.xlabel('Status')
plt.ylabel('Count')
plt.show()

# 6. Correlation Heatmap After Preprocessing
plt.figure(figsize=(10, 8))
sns.heatmap(df.corr(numeric_only=True), annot=True, cmap='coolwarm', linewidths=0.5)
plt.title('Correlation Heatmap After Preprocessing')
plt.show()

