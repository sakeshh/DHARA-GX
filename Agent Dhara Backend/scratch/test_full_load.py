import os
import sys
import time
from pathlib import Path

# Set up project path
PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_DIR / ".env")
except ImportError:
    pass

from connectors.azure_blob_storage import AzureBlobStorageConnector

try:
    conn_cfg = {
        "container": os.environ.get("AZURE_ASSESSMENT_CONTAINER")
    }
    connector = AzureBlobStorageConnector(conn_cfg)
    
    print("\n--- TEST 1: Full download & cache write ---")
    start_time = time.time()
    # Loading Taxidata.csv fully (max_rows=None, max_bytes=None)
    df = connector.load_blob("Taxidata.csv")
    duration_1 = time.time() - start_time
    print(f"Full load completed in {duration_1:.2f} seconds.")
    print(f"DataFrame shape: {df.shape}")
    
    print("\n--- TEST 2: Second load (Cache hit verification) ---")
    start_time = time.time()
    df2 = connector.load_blob("Taxidata.csv")
    duration_2 = time.time() - start_time
    print(f"Second full load completed in {duration_2:.2f} seconds.")
    print(f"DataFrame shape: {df2.shape}")
    
    if duration_2 < 5.0:
        print("\n[SUCCESS] Local cache working correctly and loaded near-instantly!")
    else:
        print("\n[WARNING] Cache did not load as fast as expected.")
        
except Exception as e:
    import traceback
    traceback.print_exc()
