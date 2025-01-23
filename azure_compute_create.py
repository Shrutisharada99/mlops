from azureml.core import Workspace
from azureml.core.compute import AmlCompute, ComputeTarget

# Connect to your workspace
ws = Workspace.from_config()

# Define compute configuration
compute_name = "cpu-cluster"
compute_config = AmlCompute.provisioning_configuration(
    vm_size="STANDARD_DS1_V2",  # VM size
    min_nodes=0,
    max_nodes=5,  # Adjust as needed
)

# Create compute target if it doesn't exist
if compute_name not in ws.compute_targets:
    compute_target = ComputeTarget.create(ws, compute_name, compute_config)
    compute_target.wait_for_completion(show_output=True)
else:
    compute_target = ws.compute_targets[compute_name]

print(f"Compute target {compute_name} is ready.")