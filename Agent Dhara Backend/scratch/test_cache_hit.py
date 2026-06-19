import os
import sys
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_DIR / ".env")
except ImportError:
    pass

from connectors.azure_blob_storage import AzureBlobStorageConnector

conn_cfg = {
    "container": os.environ.get("AZURE_ASSESSMENT_CONTAINER")
}
connector = AzureBlobStorageConnector(conn_cfg)

print("--- loading 5 rows ---")
start = time.time()
df5 = connector.load_blob("Taxidata.csv", max_rows=5)
print(f"Loaded 5 rows in {time.time() - start:.4f} seconds. Shape: {df5.shape}")

print("\n--- loading 10000 rows ---")
start = time.time()
df10k = connector.load_blob("Taxidata.csv", max_rows=10000)
print(f"Loaded 10k000 rows in {time.time() - start:.4f} seconds. Shape: {df10k.shape}")
