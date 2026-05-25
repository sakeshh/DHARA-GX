import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.etl_pipeline.llm_codegen import _consolidate_and_filter_datasets

# Setup mock datasets and metadata
datasets = {
    "dbo.students": {
        "steps": [
            {"action": "trim", "column": "credits", "order": 1},
            {"action": "trim", "column": "student_name", "order": 2},
            {"action": "lowercase", "column": "student_name", "order": 3},
            {"action": "flag_outliers", "column": "phone", "order": 4},
            {"action": "flag_outliers", "column": "credits", "order": 5},
            {"action": "clip_or_flag", "column": "credits", "order": 6},
        ]
    }
}

source_metadata = {
    "dbo.students": {
        "columns": {
            "credits": {"dtype": "int"},
            "student_name": {"dtype": "nvarchar"},
            "phone": {"dtype": "nvarchar"},
        }
    }
}

cleaned = _consolidate_and_filter_datasets(datasets, source_metadata)
steps = cleaned["dbo.students"]["steps"]

print("Pre-processed steps:")
for s in steps:
    print(f"Order: {s['order']}, Action: {s['action']}, Column: {s['column']}")

# Verify
assert not any(s["action"] == "trim" and s["column"] == "credits" for s in steps), "Trim on credits was not filtered out!"
assert not any(s["action"] == "flag_outliers" and s["column"] == "phone" for s in steps), "Outliers on phone was not filtered out!"
assert len([s for s in steps if s["column"] == "credits" and s["action"] in ("flag_outliers", "clip_or_flag")]) == 1, "Duplicate outlier steps on credits were not consolidated!"
print("\nALL PRE-PROCESSING CHECKS PASSED!")
