"""
convo_etl_guidance node for chat_graph.py.

Wired into the dispatch map in chat_graph.py under the key 'convo_etl_guidance'.
Reads the latest assessment from session and calls the ETL pipeline to build a plan
+ generate Python code. Returns the code in the payload so the frontend can render
a copy-paste code block.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def _node_convo_etl_guidance(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Entry point called by chat_graph dispatch map for action='convo_etl_guidance'.

    Steps:
    1. Pull latest assessment result from session context.
    2. Build an ETL plan using agent.etl_pipeline.planner.build_etl_plan.
    3. Generate Python code using agent.etl_pipeline.python_codegen.generate_python_etl.
    4. Return reply + payload with the generated code so the UI can show a copy-paste block.
    """
    ctx: Dict[str, Any] = (state.get("session") or {}).get("context") or {}
    result: Optional[Dict[str, Any]] = ctx.get("last_assessment_result")

    if not isinstance(result, dict):
        return {
            "reply": (
                "🚧 No assessment found in this session yet.\n\n"
                "Please generate a **Data Quality Report** first, then ask for ETL code — "
                "I’ll build a ready-to-run Python script based on the issues found."
            ),
            "payload": {"step": "etl_guidance", "code": None},
        }

    # Build ETL plan from the assessment
    try:
        from agent.etl_pipeline.planner import build_etl_plan
        from agent.etl_pipeline.python_codegen import generate_python_etl

        plan = build_etl_plan(result, business_rules={})
        code = generate_python_etl(plan, result)
    except Exception as exc:
        return {
            "reply": f"❌ Could not generate ETL code: {exc}",
            "payload": {"step": "etl_guidance", "code": None, "error": str(exc)},
        }

    datasets: List[str] = list((plan.get("datasets") or {}).keys())
    manual: List[Dict[str, Any]] = plan.get("manual_review") or []

    # Build a concise chat reply
    lines = [
        f"💾 **ETL code generated** for **{len(datasets)}** dataset(s): "
        + ", ".join(f"`{d}`" for d in datasets[:6])
        + (f" …(+{len(datasets)-6} more)" if len(datasets) > 6 else ""),
        "",
        "The script is ready to **copy–paste**. It includes:",
        "- One `transform_<dataset>(df)` function per dataset",
        "- Runtime guards for required/non-nullable columns",
        "- Valid-values filters (if business rules set)",
        "- A `# HOW TO USE` block at the bottom showing exactly how to run it",
    ]
    if manual:
        lines.append("")
        lines.append(f"⚠️ **{len(manual)} item(s) need manual review** before running in production — listed as `#` comments at the top of the file.")

    lines += [
        "",
        "*Ask for **SQL ETL** or **PySpark** if you need a different target format.*",
    ]

    reply = "\n".join(lines)

    from typing import TYPE_CHECKING
    try:
        from agent.chat_graph import _flow_options  # type: ignore
        options = _flow_options(
            {"id": "report", "text": "📄 Regenerate report", "send": "generate report"},
            {"id": "clean", "text": "🧹 Cleaning recommendations", "send": "cleaning recommendations"},
            {"id": "transform", "text": "🛠️ Suggested transformations", "send": "suggested transformations"},
            {"id": "back", "text": "🔙 Back", "send": "back"},
            {"id": "restart", "text": "✅ Restart", "send": "restart"},
        )
    except Exception:
        options = []

    return {
        "reply": reply,
        "payload": {
            "step": "etl_guidance",
            "etl_code": code,
            "plan_id": str(plan.get("plan_id") or ""),
            "datasets": datasets,
            "manual_review_count": len(manual),
            "options": options,
        },
    }
