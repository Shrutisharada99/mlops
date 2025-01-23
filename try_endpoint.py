import requests
import pandas as pd
import json

# Azure ML endpoint and API key
phy_endpoint = "https://ml-workspace-hbiua.eastus2.inference.ml.azure.com/score"
phy_api_key = "PKZgNeP85XzH9119kesm1chdSPORCNlI"

# Header with authentication if needed (e.g., an Azure ML API key)
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer {phy_api_key}"
}

# Input data in the required format
input_data = {
    "data": [
        {
            "steps": 0.8207214285714287,
            "distance_walked": 0.8307692307692308,
            "very_active_distance": 0.39999999999999997,
            "moderately_active_distance": 0.5,
            "lightly_active_distance": 0.6400000000000001,
            "calories_burnt": 0.14642240203654877
        }
    ]
}

# Send the POST request
response = requests.post(phy_endpoint, headers=headers, data=json.dumps(input_data))

# Output the prediction
print(response.status_code)
print(response.json())