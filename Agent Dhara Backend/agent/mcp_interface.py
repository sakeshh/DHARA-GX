"""MCP-facing interface to the Intelligent Data Assessment Engine.

This module wraps load_and_profile() for use by MCP servers and the chat interface.
It accepts config_text (YAML/JSON string), parses it, and calls load_and_profile.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

import yaml

from agent.intelligent_data_assessment import load_and_profile


def _parse_config_text(config_text: str) -> Dict[str, Any]:
    """Parse YAML or JSON config text into a dict."""
    if not config_text or not config_text.strip():
        return {}
    try:
        return yaml.safe_load(config_text) or {}
    except Exception:
        pass
    try:
        return json.loads(config_text)
    except Exception:
        pass
    return {}


def run_assessment(
    config_text: str,
    *,
    additional_data: Optional[Dict[str, Any]] = None,
    dq_thresholds_path: Optional[str] = None,
    job_id: Optional[str] = None,
    max_rows: Optional[int] = None,
) -> Dict[str, Any]:
    """Run the full data-quality assessment from the given config string.

    Config is the contents of sources.yaml/.json. additional_data is merged in
    (e.g. DataFrames loaded from Azure Blob). Returns the same structure as load_and_profile().
    """
    cfg = _parse_config_text(config_text)
    source_cfg = cfg.get("source", cfg) if isinstance(cfg, dict) else {}
    return load_and_profile(
        source_cfg,
        additional_data=additional_data or {},
        dq_thresholds_path=dq_thresholds_path,
        job_id=job_id,
        max_rows=max_rows,
    )


def load_selected_blob_datasets(
    config_text: str,
    location_index: int = 0,
    blob_names: Optional[list] = None,
    max_rows: Optional[int] = None,
    max_bytes: Optional[int] = None,
) -> Dict[str, Any]:
    """Load specific blobs from Azure Blob Storage into DataFrames."""
    from agent.azure_blob_loader import load_blob_datasets

    cfg = _parse_config_text(config_text)
    source_cfg = cfg.get("source", cfg) if isinstance(cfg, dict) else {}
    locations = source_cfg.get("locations") or []
    blob_locs = [loc for loc in locations if (loc.get("type") or "").lower() == "azure_blob"]

    if not blob_locs:
        return {}

    loc = blob_locs[location_index] if location_index < len(blob_locs) else blob_locs[0]
    connection_string = loc.get("connection_string") or ""
    container = loc.get("container") or ""
    prefix = loc.get("prefix") or ""

    return load_blob_datasets(
        connection_string=connection_string,
        container=container,
        prefix=prefix,
        blob_names=blob_names,
        max_rows=max_rows,
        max_bytes=max_bytes,
    )
