import time
import os
import sys

# Add backend dir to path to import agent modules
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from agent.model_config import load_llm_config
from agent.etl_pipeline.llm_codegen import _get_llm_client

def test_llm():
    print("Loading LLM config...")
    cfg = load_llm_config(purpose="etl_codegen")
    if not cfg:
        print("ERROR: load_llm_config returned None!")
        return
    print(f"Provider: {cfg.provider}")
    print(f"Model/Deployment: {cfg.model}")
    print(f"Endpoint: {cfg.endpoint}")
    print(f"API Version: {cfg.api_version}")
    print(f"API Key (first 5 chars): {cfg.api_key[:5]}...")

    client, model = _get_llm_client()
    if not client:
        print("ERROR: _get_llm_client returned None!")
        return

    print("Sending test request to LLM...")
    t0 = time.time()
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "user", "content": "Respond with only one word: hello"}
            ],
            max_tokens=10
        )
        latency = time.time() - t0
        print("SUCCESS!")
        print(f"Response: {response.choices[0].message.content}")
        print(f"Latency: {latency:.2f} seconds")
    except Exception as e:
        latency = time.time() - t0
        print(f"FAILED after {latency:.2f} seconds!")
        print(f"Error: {e}")

if __name__ == "__main__":
    # Explicitly load .env if not loaded
    from dotenv import load_dotenv
    _HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(_HERE, ".env"), override=True)
    
    test_llm()
