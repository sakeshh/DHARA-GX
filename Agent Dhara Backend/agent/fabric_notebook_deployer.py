"""
fabric_notebook_deployer.py - Deploys PySpark ETL code to Microsoft Fabric Notebooks (one notebook per dataset) and triggers runs.

Uses the FabricAPIClient to create or update notebooks, trigger execution,
and poll for run status.
"""

from __future__ import annotations

import os
import re
import ast
import time
import logging
from typing import Dict, Any, List, Optional

from agent.fabric_api_client import FabricAPIClient
from agent.blob_fabric_registry import make_safe_shortcut_name

logger = logging.getLogger("agent.fabric_notebook_deployer")

def _clean_env_value(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1].strip()
    return s or None

def _customize_code_for_dataset(pyspark_code: str, target_ds: str) -> str:
    """
    Modifies the run_pipeline execution in the pyspark_code to only load and write
    for target_ds, making the notebook execution fully independent.
    """
    lines = []
    in_run_pipeline = False
    for line in pyspark_code.splitlines():
        # Check if we are inside run_pipeline definition
        if line.strip().startswith("def run_pipeline("):
            in_run_pipeline = True
            lines.append(line)
            continue
        elif in_run_pipeline and line.startswith("def "):
            in_run_pipeline = False
            
        if in_run_pipeline:
            # If it's a load, transform, log or write line for another dataset, comment it out
            # We look for dfs["other_ds"] or if "other_ds" in dfs references
            match = re.search(r'dfs\["([^"]+)"\]|dfs\[\'([^\']+)\'\]|if\s+"([^"]+)"\s+in\s+dfs|if\s+\'([^\']+)\'\s+in\s+dfs', line)
            if match:
                matched_ds = next(g for g in match.groups() if g is not None)
                if matched_ds != target_ds:
                    # Comment it out
                    lines.append("    # [Disabled for independent run] " + line.strip())
                    continue
        lines.append(line)
    return "\n".join(lines)

def deploy_and_run_notebook(
    session_id: str,
    pyspark_code: str,
    lakehouse_id: Optional[str] = None,
    notebook_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Deploys one Microsoft Fabric Notebook per dataset in the PySpark script,
    attaches the default lakehouse, and triggers concurrent run jobs.
    """
    workspace_id = _clean_env_value(os.getenv("FABRIC_WORKSPACE_ID"))
    lh_id = lakehouse_id or _clean_env_value(os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID"))
    
    if not workspace_id or not lh_id:
        err_msg = "Missing FABRIC_WORKSPACE_ID or FABRIC_LAKEHOUSE_NAME in environment."
        logger.error(err_msg)
        return {"ok": False, "error": "MISSING_CONFIG", "message": err_msg}

    client = FabricAPIClient()
    
    # 1. Parse dataset names from generated PySpark code
    ds_match = re.search(r"DATASETS\s*=\s*(\[.*?\])", pyspark_code)
    datasets: List[str] = []
    if ds_match:
        try:
            datasets = ast.literal_eval(ds_match.group(1))
        except Exception as e:
            logger.warning(f"Could not parse DATASETS array from code: {e}")
            
    if not datasets:
        datasets = ["clean"]

    logger.info(f"Deploying {len(datasets)} dataset notebook(s) to Fabric Workspace '{workspace_id}' (Lakehouse: {lh_id})")
    
    runs = []
    for ds_name in datasets:
        # Generate safe clean name, e.g. "orders.csv" -> "orders"
        safe_name = make_safe_shortcut_name(ds_name)
        if notebook_name:
            nb_name = f"{notebook_name}_{safe_name}"
        else:
            nb_name = f"dhara_clean_{safe_name}"
        
        # Customize code to run ONLY this dataset
        custom_code = _customize_code_for_dataset(pyspark_code, ds_name)
        
        # Step 1a: Deploy or update notebook
        logger.info(f"Deploying notebook '{nb_name}'...")
        deploy_res = client.create_or_update_notebook(
            notebook_name=nb_name,
            pyspark_code=custom_code,
            lakehouse_id=lh_id
        )
        
        if not deploy_res.get("ok"):
            return deploy_res

        notebook_id = deploy_res["id"]
        
        # Step 1b: Trigger run
        try:
            logger.info(f"Triggering Notebook job run for notebook ID '{notebook_id}' ({nb_name})...")
            run_id = client.trigger_notebook_run(notebook_id)
            
            fabric_url = f"https://app.fabric.microsoft.com/groups/{workspace_id}/notebooks/{notebook_id}"
            if client.mock_mode:
                fabric_url = "#mock-mode"
                
            runs.append({
                "notebook_id": notebook_id,
                "notebook_name": nb_name,
                "dataset": ds_name,
                "run_id": run_id,
                "fabric_url": fabric_url
            })
        except Exception as e:
            logger.exception(f"Failed to trigger job run for notebook '{nb_name}': {e}")
            return {
                "ok": False,
                "error": "TRIGGER_RUN_FAILED",
                "message": f"Failed to trigger notebook run for '{nb_name}': {e}",
                "notebook_id": notebook_id
            }
            
    return {
        "ok": True,
        "runs": runs,
        "message": f"Successfully deployed {len(runs)} notebook(s) and triggered execution in Fabric."
    }

def poll_multiple_notebooks_status(
    runs: List[Dict[str, Any]],
    timeout_seconds: int = 600,
    poll_interval: int = 10
) -> Dict[str, Any]:
    """
    Polls all running notebooks in parallel until they complete.
    """
    client = FabricAPIClient()
    start_time = time.time()
    pending_runs = list(runs)
    completed_runs = []
    failed_runs = []
    
    logger.info(f"Polling status for {len(pending_runs)} running notebook(s)...")
    while time.time() - start_time < timeout_seconds and pending_runs:
        for run in list(pending_runs):
            notebook_id = run["notebook_id"]
            run_id = run["run_id"]
            
            status_res = client.get_run_status(notebook_id, run_id)
            status = status_res.get("status")
            
            if status == "Succeeded":
                logger.info(f"Notebook '{run['notebook_name']}' completed successfully.")
                completed_runs.append(run)
                pending_runs.remove(run)
            elif status == "Failed":
                logger.error(f"Notebook '{run['notebook_name']}' failed. Error: {status_res.get('error')}")
                run_err = run.copy()
                run_err["error"] = status_res.get("error")
                failed_runs.append(run_err)
                pending_runs.remove(run)
                
        if pending_runs:
            time.sleep(poll_interval)
            
    if pending_runs:
        logger.warning(f"{len(pending_runs)} notebook(s) timed out after {timeout_seconds}s.")
        for run in pending_runs:
            run_err = run.copy()
            run_err["error"] = "Execution timed out"
            failed_runs.append(run_err)
            
    if failed_runs:
        err_details = "; ".join([f"{r['notebook_name']}: {r['error']}" for r in failed_runs])
        return {
            "status": "Failed",
            "ok": False,
            "error": "NOTEBOOK_EXECUTION_FAILED",
            "message": f"One or more notebook runs failed: {err_details}",
            "failed_runs": failed_runs,
            "completed_runs": completed_runs
        }
        
    return {
        "status": "Succeeded",
        "ok": True,
        "completed_runs": completed_runs
    }
