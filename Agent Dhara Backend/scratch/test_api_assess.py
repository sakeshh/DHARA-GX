import os
import urllib.request
import json
from dotenv import load_dotenv

load_dotenv()

token = os.environ.get("BACKEND_AUTH_TOKEN", "change-me-dev")
url = "http://127.0.0.1:8000/assess"

payload = {
    "session_id": "1952cb8f-1b1f-47fc-a647-9769b8742b11",
    "sources": ["dbo.courses_raw", "dbo.students_raw"],
    "user_request": "assess selected tables"
}

headers = {
    "Content-Type": "application/json",
    "X-Backend-Token": token
}

req = urllib.request.Request(
    url,
    data=json.dumps(payload).encode('utf-8'),
    headers=headers,
    method='POST'
)

print(f"Sending POST to {url} with session_id: {payload['session_id']}...")
try:
    with urllib.request.urlopen(req) as response:
        status = response.status
        body = response.read().decode('utf-8')
        print(f"HTTP Status: {status}")
        
        result_data = json.loads(body)
        print("Response Keys:", list(result_data.keys()))
        
        if result_data.get("ok"):
            res = result_data.get("result", {})
            print("Assessment OK! Result Keys:", list(res.keys()))
            datasets = res.get("datasets", {})
            print("Datasets found in assessment:", list(datasets.keys()))
            for name, meta in datasets.items():
                print(f" - {name}: {meta.get('row_count')} rows, {meta.get('column_count')} columns")
        else:
            print("Assessment failed in response:", result_data)
            
except Exception as e:
    print("API call failed:", e)
