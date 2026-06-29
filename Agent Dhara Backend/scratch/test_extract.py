import os
import sys
import asyncio
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Reconfigure stdout to support UTF-8 characters
sys.stdout.reconfigure(encoding='utf-8')

from agent.langgraph_orchestrator import _node_extract_async

async def main():
    state = {
        "user_request": "assess selected tables",
        "sources_path": "config/sources.yaml",
        "selected_sources": [],
        "session_id": "1952cb8f-1b1f-47fc-a647-9769b8742b11",
        "job_id": "test_extraction_debug",
    }
    
    print("Running _node_extract_async...")
    try:
        res = await _node_extract_async(state)
        print("\n--- Extraction Results ---")
        print("Selected location count:", res.get("selected_location_count"))
        print("Extractions count:", len(res.get("extractions") or []))
        print("Extraction errors:", res.get("extraction_errors"))
    except Exception as e:
        print("Exception raised during execution:", e)

if __name__ == "__main__":
    asyncio.run(main())
