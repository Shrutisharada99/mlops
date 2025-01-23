import time
import numpy as np
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData
import json

import logging
logging.basicConfig(level=logging.INFO)

CONNECTION_STRING = 'Endpoint=sb://ingestion-streaming-data.servicebus.windows.net/'
EVENT_HUB_NAME = 'my-recepient-streamer'

try:
    # Create a producer client
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )

    while True:

        n_samples = 1

        # 1. Generate Steps (0 to 14000, ideal: 10000)
        steps = np.clip(np.random.normal(loc=10000, scale=3000, size=n_samples), 0, 14000)

        # 2. Generate Distance Walked (0 to 6.5, >4.5 ideal)
        distance_walked = np.clip(steps * 0.0005 + np.random.normal(0, 0.5, n_samples), 0, 6.5)

        # 3. Generate Very Active Distance (0 to 3, 1.5-3 ideal, derived from distance_walked)
        very_active_distance = np.clip(np.random.uniform(0.2, 0.5, n_samples) * distance_walked, 0, 3)

        # 4. Generate Moderately Active Distance (0 to 6, 3-6 ideal, derived from distance_walked)
        moderately_active_distance = np.clip(np.random.uniform(0.4, 0.8, n_samples) * distance_walked, 0, 6)

        # 5. Lightly Active Distance (ensuring sum matches distance_walked)
        lightly_active_distance = np.clip(distance_walked - (very_active_distance + moderately_active_distance), 0, 6)

        # 6. Generate Calories Burnt
        calories_burnt = np.where(
            np.random.rand(n_samples) < 0.2,  # 20% high-calorie burn group
            np.random.uniform(800, 2500, n_samples),
            np.random.uniform(300, 700, n_samples)  # 80% within 300-700
        )

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

        # 11. Generate Heart Rate Condition (Categorical with weighted probabilities)
        heart_rate_condition = np.random.choice(
            ["Skipped beats", "Extra beats", "Mixed", "Uneven timing", "Normal"],
            size=n_samples,
            p=[0.1, 0.1, 0.1, 0.1, 0.6]
        )

        # 12. Generate Resting Heart Rate (40-100, ideal: 60-100)
        resting_heart_rate = np.clip(np.random.normal(loc=70, scale=15, size=n_samples), 40, 100)

        # 13. Generate Current Heart Rate (Rest)
        # Ideal case: within ±4, Non-ideal: within ±10
        current_heart_rate_rest = resting_heart_rate + np.where(
            np.random.rand(n_samples) < 0.7,  # 70% ideal
            np.random.uniform(-4, 4, n_samples),  # Ideal range
            np.random.uniform(-10, 10, n_samples)  # Non-ideal range
        )

        # 14. Generate Increase During Activity (60-200, ideal: 100-170)
        increase_during_activity = np.clip(np.random.normal(loc=130, scale=30, size=n_samples), 60, 200)

        # 15. Generate Post-Activity Beat Drop (5-45, ideal: >12)
        post_activity_beat_drop = np.clip(np.random.normal(loc=20, scale=10, size=n_samples), 5, 45)

        # 16. Air Quality Index (AQI) Data
        air_quality = np.clip(np.random.normal(loc=3, scale=1, size=n_samples), 1, 5)

        physical_activity_score = 10 - (
            2 * np.abs(steps - 10000) / 10000 +
            1.5 * np.abs(distance_walked - 5) / 5 +
            2 * np.abs(very_active_distance - 2.25) / 2.25 +
            2 * np.abs(moderately_active_distance - 4.5) / 4.5 +
            2 * np.abs(lightly_active_distance - 4.5) / 4.5 +
            1.5 * np.clip(300 - calories_burnt, 0, 300) / 300
        )
        physical_activity_score = np.clip(physical_activity_score, 1, 10)

        sleep_score = 10 - (
            2 * np.abs(total_sleep_duration - 8) / 8 +  # Penalize deviation from 8 hours
            1.5 * np.abs(light_sleep_stage - 4.5) / 4.5 +  # Penalize deviation from ideal light sleep
            2 * np.abs(deep_sleep_stage - 1.75) / 1.75 +  # Penalize deviation from ideal deep sleep
            2 * np.abs(rem_sleep_stage - 1.75) / 1.75 +  # Penalize deviation from ideal REM
            0.5 * number_of_awakenings  # Penalize more awakenings
        )
        sleep_score = np.clip(sleep_score, 1, 10)  # Ensure score is within 1-10

        health_management_score = (physical_activity_score + sleep_score)/2

        # Combine into DataFrame
        data = pd.DataFrame({
            "steps": np.round(steps, 1),
            "distance_walked": np.round(distance_walked, 1),
            "very_active_distance": np.round(very_active_distance, 1),
            "moderately_active_distance": np.round(moderately_active_distance, 1),
            "lightly_active_distance": np.round(lightly_active_distance, 1),
            "calories_burnt": np.round(calories_burnt, 1),
            "total_sleep_duration": np.round(total_sleep_duration, 1),
            "light_sleep_stage": np.round(light_sleep_stage, 1),
            "deep_sleep_stage": np.round(deep_sleep_stage, 1),
            "REM_sleep_stage": np.round(rem_sleep_stage, 1),
            "number_of_awakenings": number_of_awakenings,  
            "heart_rate_condition": heart_rate_condition,
            "resting_heart_rate": np.round(resting_heart_rate, 1),
            "current_heart_rate_rest": np.round(current_heart_rate_rest, 1),
            "increase_during_activity": np.round(increase_during_activity, 1),
            "post_activity_beat_drop": np.round(post_activity_beat_drop, 1),
            "air_quality": np.round(air_quality, 1),
            "health_management_score": np.round(health_management_score, 1),
        })
        
        data_json = data.to_json(orient="records")

        # Create a batch of events
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData("First event"))
        event_data_batch.add(EventData("Second event"))

        # Send the batch
        producer.send_batch(event_data_batch)
        print("Events successfully sent!")

        time.sleep(100)

except Exception as e:
    print("Error:", e)
finally:
    producer.close()