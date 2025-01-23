import mlflow
from azureml.core.runconfig import RunConfiguration
from azureml.core import Workspace, Experiment, ScriptRunConfig, Environment, Dataset
from azureml.train.hyperdrive import (
    GridParameterSampling,
    HyperDriveConfig,
    PrimaryMetricGoal,
    uniform,
    choice,
)
import os
from sklearn.metrics import r2_score
from azureml.core.runconfig import DockerConfiguration
from azureml.core import Model
import numpy

# Connect to Azure ML workspace
ws = Workspace.from_config()

# Create or retrieve an Azure ML environment
try:
    env = Environment.get(workspace=ws, name="training-env")
except:
    env = Environment.from_conda_specification(name="training-env", file_path="dep.yml")
    env.register(workspace=ws)

# Enable Docker for the environment
docker_config = DockerConfiguration(use_docker=True)

param_sampling = GridParameterSampling(
    {
        "--n_estimators": choice(100,150,200),       
        "--max_depth": choice(5,7)                 
    }
)

sleep_rfr = ScriptRunConfig(
    source_directory=".",  # Directory containing train.py
    script="sleep_rfr_train.py",
    compute_target="cpu-cluster",
    environment=env,
    docker_runtime_config=docker_config
)

sleep_rgb = ScriptRunConfig(
    source_directory=".",  # Directory containing train.py
    script="sleep_rgb_train.py",
    compute_target="cpu-cluster",
    environment=env,
    docker_runtime_config=docker_config
)

runs = [sleep_rfr, sleep_rgb]

for idx, run in enumerate(runs):
    print(run)
    experiment = Experiment(ws, name=f"expt_{idx}")
    hyperdrive_config = HyperDriveConfig(
        run_config=run,
        hyperparameter_sampling=param_sampling,
        policy=None,  # Add early stopping policy if needed
        primary_metric_name='r2',
        primary_metric_goal=PrimaryMetricGoal.MAXIMIZE,
        max_total_runs=20
    )
    hyperdrive_run = experiment.submit(hyperdrive_config)
    print("Submitted HyperDrive experiment. Run ID:", hyperdrive_run.id)

    # Wait for the run to complete
    hyperdrive_run.wait_for_completion(show_output=True)