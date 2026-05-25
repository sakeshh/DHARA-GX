import time
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from agent.session_store import load_session
from agent.etl_handlers import _get_assessment, _rehydrate_plan
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.validate_sql import validate_sql_basic

def run_test():
    session_id = "f8af7093-f392-4a38-b692-d12caa902801"
    print(f"Loading session {session_id}...")
    sess = load_session(session_id)
    ctx = sess.get("context", {})
    flow = ctx.get("etl_flow", {})
    plan = flow.get("plan") # We can check the raw plan
    
    if not plan:
        print("ERROR: plan not found in session!")
        return

    plan = _rehydrate_plan(plan, ctx)
    assess = _get_assessment(sess, None)
    if not assess:
        print("ERROR: assessment_result not found in session!")
        return

    print("Generating template SQL via modified sql_codegen.py...")
    t0 = time.time()
    try:
        code = generate_sql_etl(plan, assess, dialect="tsql")
        latency = time.time() - t0
        print(f"Template SQL generation finished in {latency:.4f} seconds.")
        
        ok, errs = validate_sql_basic(code)
        print(f"Validation ok: {ok}")
        print(f"Validation errors: {errs}")
        
        # Save output for inspection
        output_path = "scratch/template_generated_output.sql"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(code)
        print(f"Wrote template output to {output_path}")
        
        # Check if dob logic or outliers got duplicated
        outlier_count = code.lower().count("sp_flag_outliers_iqr")
        dob_quarantine_count = code.lower().count("quarantine invalid dates")
        print(f"Outlier sp_flag_outliers_iqr calls: {outlier_count}")
        print(f"DOB quarantine references: {dob_quarantine_count}")
        
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    run_test()
