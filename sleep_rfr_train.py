import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from azureml.core import Workspace, Run
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
import argparse
import os
from sklearn.metrics import r2_score
import joblib

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--n_estimators", type=int, default=100)
parser.add_argument("--max_depth", type=int, default=2)
args = parser.parse_args()

# Read the Physical Activity data file
sleep_data = pd.read_csv("artifacts/training_data/sleep_quality_train_data.csv")

# Connect to Azure ML Workspace
run = Run.get_context()
ws = run.experiment.workspace

# Train-test split
X = sleep_data.drop("sleep_score", axis=1)
y = sleep_data["sleep_score"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=40)

# Train model
model = RandomForestRegressor(
    n_estimators=args.n_estimators,
    max_depth=args.max_depth
)

model.fit(X_train, y_train)

# Evaluate model
y_pred = model.predict(X_test)
r2 = r2_score(y_test, y_pred)
run.log("R2 Score", r2)
print(f"R2 Score: {r2}")

# Save the model
os.makedirs("outputs", exist_ok=True)
model_path = "outputs/sleep_rfr_model.pkl"
joblib.dump(model, model_path)

# Register the model
run.upload_file(name="outputs/sleep_rfr_model.pkl", path_or_stream=model_path)
run.register_model(
    model_name="sleep-pred-rfr-model",
    model_path="outputs/sleep_rfr_model.pkl"
)

print(f"R2 Score: {r2}")
print(f"Model registered successfully!")