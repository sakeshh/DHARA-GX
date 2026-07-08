"""
fabric_shortcut_service.py - Manages landing raw data in Fabric OneLake Files/ zone using Shortcuts.

Connects to the FabricAPIClient to create zero-copy shortcuts from Azure Blob Storage 
to the Lakehouse Files/ zone, and registers the locations in the local SQLite db.
"""

from __future__ import annotations

import os
import logging
from typing import Dict, Any, List, Optional

from agent.fabric_api_client import FabricAPIClient
from agent.blob_fabric_registry import register_shortcut, make_safe_shortcut_name

logger = logging.getLogger("agent.fabric_shortcut_service")

def _clean_env_value(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1].strip()
    return s or None

def create_shortcuts_for_blobs(
    session_id: str,
    selected_blob_paths: List[str],
    blob_account_name: Optional[str] = None,
    blob_container: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Creates a zero-copy Fabric Shortcut for each selected blob, linking it
    to the 'Files/raw/' zone of the lakehouse.
    
    Returns a list of dicts mapping each blob to its new Files/ zone OneLake path.
    """
    # Load defaults from environment if not explicitly provided
    account = blob_account_name or _clean_env_value(os.getenv("AZURE_STORAGE_ACCOUNT_NAME"))
    container = blob_container or _clean_env_value(os.getenv("AZURE_ASSESSMENT_CONTAINER") or os.getenv("AZURE_STORAGE_CONTAINER"))
    
    workspace = _clean_env_value(os.getenv("FABRIC_WORKSPACE_ID") or os.getenv("FABRIC_WORKSPACE_NAME"))
    lakehouse = _clean_env_value(os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID"))
    
    if not account or not container:
        raise ValueError("Missing required Azure Storage credentials/container configuration.")
    if not workspace or not lakehouse:
        raise ValueError("Missing required Fabric Workspace or Lakehouse configuration.")

    client = FabricAPIClient()
    results = []
    
    for blob_path in selected_blob_paths:
        # Clean paths of prefix if user passed "azure_blob:path"
        clean_blob_path = blob_path.replace("azure_blob:", "")
        
        # Safe shortcut name (must be unique inside Files/raw/ folder)
        shortcut_name = make_safe_shortcut_name(clean_blob_path)
        
        # Standard folder structure path: e.g. "Files/raw/sales_csv"
        # We append a clean extension or name to keep it readable
        files_zone_path = f"Files/raw/{shortcut_name}"
        
        # Build standard OneLake URI: abfss://workspace@onelake.dfs.fabric.microsoft.com/Lakehouse/Files/raw/sales_csv
        # Note: Lakehouse names in OneLake may require the .Lakehouse suffix. get_lakehouse_folder() resolves this.
        from connectors.fabric_lakehouse_connector import get_lakehouse_folder
        lakehouse_folder = get_lakehouse_folder(lakehouse)
        lakehouse_uri = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse_folder}/{files_zone_path}"
        
        logger.info(f"Creating Fabric shortcut: '{files_zone_path}' pointing to blob://{account}/{container}/{clean_blob_path}")
        
        # Create shortcut via REST API
        api_res = client.create_shortcut(
            shortcut_name=shortcut_name,
            target_blob_account=account,
            target_container=container,
            target_path=clean_blob_path,
            destination_path="Files/raw"
        )
        
        if api_res.get("ok"):
            register_shortcut(
                session_id=session_id,
                blob_path=blob_path, # keep original for matching
                files_zone_path=files_zone_path,
                shortcut_name=shortcut_name,
                lakehouse_uri=lakehouse_uri,
                method="shortcut"
            )
            results.append({
                "blob": blob_path,
                "files_path": files_zone_path,
                "shortcut_name": shortcut_name,
                "uri": lakehouse_uri,
                "status": "success"
            })
        else:
            err_msg = api_res.get("message", "Unknown Fabric API error")
            logger.error(f"Failed to create shortcut for '{clean_blob_path}': {err_msg}")
            results.append({
                "blob": blob_path,
                "files_path": files_zone_path,
                "shortcut_name": shortcut_name,
                "uri": lakehouse_uri,
                "status": "failed",
                "error": err_msg
            })
            
    return results
