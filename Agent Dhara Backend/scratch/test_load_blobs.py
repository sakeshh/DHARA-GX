import os
import sys
from pathlib import Path

# Set up project path
PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_DIR / ".env")
    print(".env loaded successfully.")
except ImportError:
    print("dotenv not installed, proceeding with system environment.")

# Print preflight env check (redacting keys)
keys = [
    "AZURE_STORAGE_CONNECTION_STRING",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "AZURE_STORAGE_ACCOUNT_KEY",
    "AZURE_ASSESSMENT_CONTAINER",
]
print("Environment check:")
for key in keys:
    val = os.environ.get(key)
    print(f"  {key}: {'SET' if val else 'NOT SET'} (Length: {len(val) if val else 0})")

from connectors.azure_blob_storage import AzureBlobStorageConnector

try:
    conn_cfg = {
        "container": os.environ.get("AZURE_ASSESSMENT_CONTAINER")
    }
    print(f"\nInitializing AzureBlobStorageConnector with container: {conn_cfg['container']}...")
    connector = AzureBlobStorageConnector(conn_cfg)
    print("Connector initialized successfully.")
    
    print("\nListing blobs...")
    blobs = connector.list_blobs()
    print(f"Discovered {len(blobs)} blobs:")
    for b in blobs:
        print(f"  - {b}")
        
    if blobs:
        first_blob = blobs[0]
        print(f"\nAttempting to load first blob: {first_blob}...")
        df = connector.load_blob(first_blob, max_rows=5)
        print("Load completed.")
        print(f"DataFrame shape: {df.shape}")
        print("DataFrame head:")
        print(df.head())
    else:
        print("No blobs to test loading.")
except Exception as e:
    import traceback
    print("\nException encountered:")
    traceback.print_exc()
