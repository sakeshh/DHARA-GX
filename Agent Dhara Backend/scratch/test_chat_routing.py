import os
import sys
import json
from pathlib import Path

# Set up project path
PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_DIR / ".env")
    print(".env loaded.")
except ImportError:
    print("dotenv not installed.")

from agent.chat_graph import run_chat

# Test scenario:
# 1. Set context to simulated Azure Blob source
from agent.session_store import load_session, save_session
session_id = "test_routing_session"
sess = load_session(session_id)
# Simulate that the user has selected Azure Blob source
sess.setdefault("context", {})["selected_source"] = "azure_blob"
sess["context"]["selected_source_index"] = 1
sess["context"]["last_blob_list"] = ["Taxidata.csv", "data_quality_issues.csv"]
save_session(sess)

try:
    print("\nRunning chat with message='list files'...")
    res = run_chat(session_id=session_id, message="list files")
    print("\nRouting result:")
    print(f"Reply snippet: {res.get('reply')}")
    print(f"Payload step: {res.get('payload', {}).get('step')}")
    print(f"Payload keys: {list(res.get('payload', {}).keys())}")
except Exception as e:
    import traceback
    traceback.print_exc()
