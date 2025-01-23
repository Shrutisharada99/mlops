import json
import joblib
import numpy as np
import os
from azureml.core.model import Model

# The init function is called once when the endpoint is started
def init():
    global model
    # Retrieve the path to the model file using the Model class
    model_path = Model.get_model_path("phy_act_rgb_model")  # Replace "my_model_name" with your model name
    model = joblib.load(model_path)  # Load the model into memory

# The run function is called for every request to the endpoint
def run(data):
    try:
        # Parse the input JSON data
        input_data = json.loads(data)
        input_array = np.array(input_data["data"])  # Assuming input is in {"data": [[...]]} format

        # Perform prediction
        predictions = model.predict(input_array)

        # Return the predictions as JSON
        return json.dumps({"predictions": predictions.tolist()})
    except Exception as e:
        error = str(e)
        return json.dumps({"error": error})