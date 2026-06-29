import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from agent.session_store import load_session

session_id = "1952cb8f-1b1f-47fc-a647-9769b8742b11"
sess = load_session(session_id)
flow = sess.get("context", {}).get("etl_flow", {})
print(f"\n--- Session '{session_id}' ETL Flow ---")
print("Target Engine:", flow.get("target_engine"))
print("Phase:", flow.get("phase"))
print("Validation OK:", flow.get("validation_ok"))
print("Artifact Rel Path:", flow.get("artifact_rel_path"))

sql_code = flow.get("code")
if sql_code:
    print(f"\nSQL code length: {len(sql_code)} characters")
    print("SQL code snippet (first 300 chars):")
    print(sql_code[:300])
else:
    print("\nNo SQL code found in session.")
