"""
mirror_service.py - Orchestrates mirroring Azure Blob storage locations to Microsoft Fabric OneLake via Shortcuts.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, List

from agent.blob_fabric_registry import get_shortcut
from agent.fabric_shortcut_service import create_shortcuts_for_blobs

logger = logging.getLogger("agent.mirror_service")

def mirror_blobs_to_fabric(
    session_id: str,
    blob_paths: List[str]
) -> Dict[str, Any]:
    """
    Ensures zero-copy shortcuts are established in Microsoft Fabric OneLake for the given blob paths.
    Checks existing shortcuts first to avoid redundant API calls.
    
    Returns a dictionary mapping blob_path -> shortcut metadata/status.
    """
    results = {}
    to_create = []
    
    # 1. Identify which paths are already mirrored
    for path in blob_paths:
        existing = get_shortcut(session_id, path)
        if existing:
            results[path] = {
                "lakehouse_uri": existing["lakehouse_uri"],
                "shortcut_name": existing["shortcut_name"],
                "files_zone_path": existing["files_zone_path"],
                "status": "already_mirrored",
                "ok": True
            }
            logger.info(f"Blob '{path}' is already mirrored to Fabric in session '{session_id}'.")
        else:
            to_create.append(path)
            
    # 2. Call shortcut service for any un-mirrored paths
    if to_create:
        logger.info(f"Creating shortcuts for {len(to_create)} blob paths in session '{session_id}'...")
        create_results = create_shortcuts_for_blobs(
            session_id=session_id,
            selected_blob_paths=to_create
        )
        
        for item in create_results:
            blob = item["blob"]
            if item.get("status") == "success":
                results[blob] = {
                    "lakehouse_uri": item["uri"],
                    "shortcut_name": item["shortcut_name"],
                    "files_zone_path": item["files_path"],
                    "status": "mirrored",
                    "ok": True
                }
            else:
                results[blob] = {
                    "status": "failed",
                    "error": item.get("error", "Failed to create Fabric shortcut"),
                    "ok": False
                }
                
    return results
