import os
import sys
import pandas as pd
from dotenv import load_dotenv

# Ensure we can import modules from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env variables
load_dotenv()

print("--- Agent Dhara Fabric Verification Script ---")
print("DHARA_FABRIC_MIRROR_ENABLED:", os.getenv("DHARA_FABRIC_MIRROR_ENABLED"))
print("FABRIC_WORKSPACE_ID:", os.getenv("FABRIC_WORKSPACE_ID"))
print("FABRIC_LAKEHOUSE_NAME:", os.getenv("FABRIC_LAKEHOUSE_NAME"))
print("FABRIC_TENANT_ID:", os.getenv("FABRIC_TENANT_ID"))

# Import connector function
try:
    from connectors.fabric_lakehouse_connector import write_to_lakehouse
    print("Connector imported successfully.")
except Exception as e:
    print(f"Failed to import connector: {e}")
    sys.exit(1)

# Create a dummy DataFrame
df = pd.DataFrame({
    "TestID": [1, 2, 3],
    "Message": ["Hello from Agent Dhara", "Fabric Connection Test", "Delta Table Success"],
    "Status": ["Active", "Pending", "Completed"]
})

print("\nAttempting to write dummy Delta table to Fabric OneLake...")
res = write_to_lakehouse(df, "dbo.TestConnectionTable")

print("\nResult:")
try:
    print(str(res))
except UnicodeEncodeError:
    # Print with non-ascii characters stripped or replaced
    print(str(res).encode('ascii', errors='replace').decode('ascii'))

