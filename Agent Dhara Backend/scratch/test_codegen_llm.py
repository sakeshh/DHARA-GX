import time
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from agent.session_store import load_session
from agent.etl_handlers import _get_assessment, _rehydrate_plan
from agent.etl_pipeline.llm_codegen import generate_etl_with_llm, is_llm_generation_error

def run_test():
    session_id = "f8af7093-f392-4a38-b692-d12caa902801"
    print(f"Loading session {session_id}...")
    sess = load_session(session_id)
    ctx = sess.get("context", {})
    flow = ctx.get("etl_flow", {})
    plan = flow.get("approved_plan")
    
    if not plan:
        print("ERROR: approved_plan not found in session!")
        return

    plan = _rehydrate_plan(plan, ctx)
    assess = _get_assessment(sess, None)
    if not assess:
        print("ERROR: assessment_result not found in session!")
        return

    print("Re-running generate_etl_with_llm on Azure OpenAI...")
    t0 = time.time()
    try:
        code = generate_etl_with_llm(
            plan=plan,
            assessment=assess,
            engine="sql",
            sql_dialect="tsql",
            output_mode="dataframe_only",
            output_path=None
        )
        latency = time.time() - t0
        print(f"Generation finished in {latency:.2f} seconds.")
        print(f"Is LLM generation error: {is_llm_generation_error(code)}")
        
        from agent.etl_pipeline.validate_sql import validate_sql_basic
        ok, errs = validate_sql_basic(code)
        print(f"Validation ok: {ok}")
        print(f"Validation errors: {errs}")
        
        # Write LLM output to a file for inspection
        with open("scratch/llm_generated_output.sql", "w", encoding="utf-8") as f:
            f.write(code)
        print("Wrote LLM output to scratch/llm_generated_output.sql")
        
        print("First 500 characters of output:")
        print("-" * 40)
        print(code[:500])
        print("-" * 40)
        
        if is_llm_generation_error(code):
            print("Full error text:")
            print(code)
    except Exception as e:
        latency = time.time() - t0
        print(f"FAILED after {latency:.2f} seconds!")
        print(f"Exception: {e}")

if __name__ == "__main__":
    from dotenv import load_dotenv
    _HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(_HERE, ".env"), override=True)
    run_test()
