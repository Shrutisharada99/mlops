from confluent_kafka import Producer
import pandas as pd
import time
import numpy as np

# Kafka configuration
config = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',  # Found in Confluent Cloud Cluster settings
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'VPVOVS6AIQT3V577',  # API key from Confluent
    'sasl.password': 'CscjmODsggXkRt214VwH6G7E8Pv0hpyWTKBwCsS/9zdBf92h3n6ziMffCczZUgAq',  # API secret from Confluent
}

# Create a Kafka producer
producer = Producer(config)
topic = "Wellness_score"  # Replace with your Kafka topic

# Define a callback for delivery report
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}")

i = -1
while True:
    n_samples = 1
    i+=1

    total_sleep_duration = np.clip(np.random.normal(loc=8, scale=2, size=n_samples), 0, 13)

    # 7. Generate Light, Deep, and REM Stages (columns 2, 3, 4) that sum to Total Sleep Duration
    light_sleep_proportion = np.random.uniform(0.4, 0.6, n_samples)  # Light sleep typically 40-60%
    deep_sleep_proportion = np.random.uniform(0.1, 0.3, n_samples)   # Deep sleep typically 10-30%
    rem_sleep_proportion = 1 - (light_sleep_proportion + deep_sleep_proportion)  # Remaining for REM

    #8. Ensure REM proportion is realistic (0-0.3)
    rem_sleep_proportion = np.clip(rem_sleep_proportion, 0.1, 0.3)

    # 9. Scale proportions to total sleep duration
    light_sleep_stage = np.round(light_sleep_proportion * total_sleep_duration, 1)
    deep_sleep_stage = np.round(deep_sleep_proportion * total_sleep_duration, 1)
    rem_sleep_stage = np.round(rem_sleep_proportion * total_sleep_duration, 1)

    #. Adjust to ensure the sum equals total_sleep_duration
    sum_stages = light_sleep_stage + deep_sleep_stage + rem_sleep_stage
    correction = total_sleep_duration - sum_stages

    # Distribute correction randomly across light, deep, and REM
    light_sleep_stage += correction * np.random.uniform(0.4, 0.6, n_samples)
    deep_sleep_stage += correction * np.random.uniform(0.2, 0.4, n_samples)
    rem_sleep_stage += correction * np.random.uniform(0.2, 0.4, n_samples)

    # Clip values to their bounds and ensure sum equals total_sleep_duration
    light_sleep_stage = np.clip(light_sleep_stage, 0, 7)
    deep_sleep_stage = np.clip(deep_sleep_stage, 0, 3)
    rem_sleep_stage = np.clip(rem_sleep_stage, 0, 3)
    sum_stages = light_sleep_stage + deep_sleep_stage + rem_sleep_stage
    light_sleep_stage += (total_sleep_duration - sum_stages) * 0.5
    deep_sleep_stage += (total_sleep_duration - sum_stages) * 0.25
    rem_sleep_stage += (total_sleep_duration - sum_stages) * 0.25

    # 10. Generate Number of Awakenings (column 5): Uniform distribution with peak at 0-3
    number_of_awakenings = np.clip(np.random.choice(range(0, 11), size=n_samples, p=[0.3, 0.25, 0.15, 0.1, 0.05, 0.05, 0.03, 0.02, 0.02, 0.02, 0.01]), 0, 10)

    # Combine into DataFrame
    data = pd.DataFrame({
        "total_sleep_duration": np.round(total_sleep_duration, 1),
        "light_sleep_stage": np.round(light_sleep_stage, 1),
        "deep_sleep_stage": np.round(deep_sleep_stage, 1),
        "REM_sleep_stage": np.round(rem_sleep_stage, 1),
        "number_of_awakenings": number_of_awakenings,  
    })

    key = str(i)  # Use row index as key
    value = data.to_json()  # Convert row to JSON
    producer.produce(topic, key=key, value=value, callback=delivery_report)
    producer.poll(1)  # Poll to handle delivery reports
    time.sleep(10)  # Simulate real-time streaming with a delay

    # Flush producer before exiting
    producer.flush()