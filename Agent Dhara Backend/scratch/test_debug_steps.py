import time
import os
import sys
from pathlib import Path

def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

log("Starting diagnostic script...")

# Set up project path
PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

log("Loading dotenv...")
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_DIR / ".env")
    log(".env loaded.")
except Exception as e:
    log(f"dotenv error: {e}")

log("Importing azure.storage.blob...")
t0 = time.time()
from azure.storage.blob import BlobServiceClient
log(f"Imported in {time.time() - t0:.2f}s")

connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
container_name = os.environ.get("AZURE_ASSESSMENT_CONTAINER")

log(f"Container name: {container_name}")
log(f"Connection string present: {bool(connection_string)}")

if not connection_string or not container_name:
    log("Credentials or container name missing!")
    sys.exit(1)

log("Creating BlobServiceClient from connection string...")
t0 = time.time()
client = BlobServiceClient.from_connection_string(connection_string)
log(f"Client created in {time.time() - t0:.2f}s")

log("Getting container client...")
container_client = client.get_container_client(container_name)
log("Got container client.")

log("Calling list_blobs() iterator...")
t0 = time.time()
blob_iter = container_client.list_blobs()
log(f"Iterator obtained in {time.time() - t0:.2f}s")

log("Iterating over first few blobs...")
t0 = time.time()
blobs = []
try:
    # Set a timeout on the network call if possible, or just try to fetch a few
    for b in blob_iter:
        blobs.append(b.name)
        log(f"Found blob: {b.name}")
        if len(blobs) >= 5:
            break
    log(f"Iterated first few blobs in {time.time() - t0:.2f}s. Total found: {len(blobs)}")
except Exception as e:
    log(f"Failed to iterate blobs: {e}")
    sys.exit(1)

if blobs:
    first_blob = blobs[0]
    log(f"Downloading first 100 bytes of {first_blob}...")
    t0 = time.time()
    try:
        blob_client = container_client.get_blob_client(first_blob)
        data = blob_client.download_blob(offset=0, length=100).readall()
        log(f"Downloaded {len(data)} bytes in {time.time() - t0:.2f}s")
        log(f"Sample data: {data[:50]}")
    except Exception as e:
        log(f"Failed to download blob slice: {e}")
else:
    log("No blobs found in container.")
