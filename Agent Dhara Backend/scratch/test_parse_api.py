import sys
import os
import json
from pathlib import Path

# Add parent path to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from agent.business_requirements_parser import parse_requirements_to_rules

# Mock schema
schemas = {
    "customers": ["Id", "Name", "Email", "Age"]
}

req_text = "only @capgemini.com mail ids are valid"
print(f"Parsing: {req_text}")
rules = parse_requirements_to_rules(req_text, schemas)
print("Resulting rules:")
print(json.dumps(rules, indent=2))
