import sys
import os

# Add Agent Dhara Backend to python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.fixtures.blob_pair_assessment import blob_session_context, make_blob_pair_assessment
from agent.etl_pipeline.business_rules import normalize_business_rules
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.source_context import build_source_context
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.validate_sql import validate_sql_basic

print("Initializing test data...")
assess = make_blob_pair_assessment()
rules = normalize_business_rules(
    {
        "never_drop_rows": False,
        "outlier_strategy": "flag",
        "valid_values": {"department": ["engineering", "sales"]},
        "exclude_columns": []
    }
)
ctx = blob_session_context()
plan = build_etl_plan(
    assess,
    rules,
    source_context=build_source_context(ctx, assess),
)

print("\nGenerating SQL Dialect: tsql...")
code = generate_sql_etl(plan, assess, dialect="tsql")

print("\nValidation of generated SQL:")
ok, errs = validate_sql_basic(code)
print(f"Validation ok: {ok}")
print(f"Errors: {errs}")

# Write code to file for visual inspection
out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "template_generated_output.sql")
with open(out_path, "w", encoding="utf-8") as f:
    f.write(code)
print(f"\nWritten generated SQL to {out_path} for inspection.")
