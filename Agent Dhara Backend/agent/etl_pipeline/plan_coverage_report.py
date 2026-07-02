"""
Plan step coverage report: cross-references assessment issues against plan steps.
"""
from typing import Any, Dict, List

def build_coverage_report(assessment: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compares the quality issues identified in the assessment against the steps
    and manual review/blocked items defined in the ETL plan.
    Returns:
        Dict: {
            "covered": List[Dict[str, Any]],
            "uncovered": List[Dict[str, Any]],
            "coverage_pct": float
        }
    """
    covered = []
    uncovered = []
    
    def _clean_ds(ds):
        d = str(ds or "").strip()
        if not d or d.lower() == "global":
            return "_global"
        return d.lower()

    def _clean_col(col):
        c = str(col or "").strip()
        return c.lower() if c else "*"

    # 1. Collect all columns with steps in the plan
    planned_cols = set()
    datasets = (plan or {}).get("datasets") or {}
    for ds_name, ds_block in datasets.items():
        for step in (ds_block.get("steps") or []):
            col = step.get("column")
            planned_cols.add((_clean_ds(ds_name), _clean_col(col)))
                
    # 2. Collect columns in manual review
    manual_review = (plan or {}).get("manual_review") or []
    for item in manual_review:
        ds = item.get("dataset")
        col = item.get("column")
        planned_cols.add((_clean_ds(ds), _clean_col(col)))
            
    # 3. Collect columns in blocked
    blocked = (plan or {}).get("blocked") or []
    for item in blocked:
        ds = item.get("dataset")
        col = item.get("column")
        planned_cols.add((_clean_ds(ds), _clean_col(col)))

    # 3b. Collect columns in non_fixable
    non_fixable = (plan or {}).get("non_fixable") or []
    for item in non_fixable:
        ds = item.get("dataset")
        col = item.get("column")
        planned_cols.add((_clean_ds(ds), _clean_col(col)))
            
    # 4. Iterate over quality issues in the assessment
    ass_datasets = (assessment or {}).get("datasets") or {}
    total_issues = 0
    
    for ds_name, ds_meta in ass_datasets.items():
        dq_issues = list((ds_meta.get("quality") or {}).get("issues") or [])
        legacy_issues = (assessment or {}).get("data_quality_issues", {}).get("datasets", {}).get(ds_name, {}).get("issues", [])
        if legacy_issues:
            dq_issues.extend(legacy_issues)
            
        for issue in dq_issues:
            col = issue.get("column")
            total_issues += 1
            
            issue_detail = {
                "dataset": ds_name,
                "column": col or "",
                "issue_type": issue.get("type"),
                "message": issue.get("message"),
                "severity": issue.get("severity")
            }
            
            issue_key = (_clean_ds(ds_name), _clean_col(col))
            if issue_key in planned_cols:
                covered.append(issue_detail)
            else:
                uncovered.append(issue_detail)
                
    coverage_pct = 100.0
    if total_issues > 0:
        coverage_pct = round((len(covered) / total_issues) * 100, 2)
        
    return {
        "covered": covered,
        "uncovered": uncovered,
        "coverage_pct": coverage_pct
    }
