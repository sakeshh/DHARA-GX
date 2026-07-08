"""
blob_fabric_registry.py - SQLite registry for tracking Azure Blob to Fabric OneLake Shortcuts.

Tracks where Azure Blob files are mirrored or linked via Shortcuts within a session.
Reuses the database connection proxy from agent.session_store.
"""

from __future__ import annotations

import re
import time
import logging
from typing import Dict, Any, List, Optional
from agent.session_store import _connect

logger = logging.getLogger("agent.blob_fabric_registry")

def _init_db():
    """Ensure the blob_fabric_shortcuts table exists in sqlite."""
    conn = _connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS blob_fabric_shortcuts (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id      TEXT NOT NULL,
                blob_path       TEXT NOT NULL,
                files_zone_path TEXT NOT NULL,
                shortcut_name   TEXT NOT NULL,
                lakehouse_uri   TEXT NOT NULL,
                created_ts      REAL NOT NULL,
                method          TEXT NOT NULL,
                FOREIGN KEY(session_id) REFERENCES sessions(session_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_blob_fabric_shortcuts_session ON blob_fabric_shortcuts(session_id, blob_path)"
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to initialize blob_fabric_shortcuts table: {e}")
    finally:
        conn.close()

# Initialize on import
_init_db()

def make_safe_shortcut_name(blob_path: str) -> str:
    """
    Generate a clean, alphanumeric-only shortcut name from a blob path.
    Example: "raw/2024/sales.csv" -> "raw_2024_sales_csv"
    """
    # Replace non-alphanumeric characters with underscores
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', blob_path)
    # Deduplicate underscores
    safe_name = re.sub(r'_+', '_', safe_name)
    return safe_name.strip('_')

def register_shortcut(
    session_id: str,
    blob_path: str,
    files_zone_path: str,
    shortcut_name: str,
    lakehouse_uri: str,
    method: str = "shortcut"
) -> None:
    """
    Register a blob-to-Fabric-shortcut mapping. If one exists, overwrite it.
    """
    sid = (session_id or "default").strip() or "default"
    now = time.time()
    conn = _connect()
    try:
        # Check if already exists
        row = conn.execute(
            "SELECT id FROM blob_fabric_shortcuts WHERE session_id = ? AND blob_path = ?",
            (sid, blob_path)
        ).fetchone()
        
        if row:
            conn.execute(
                """
                UPDATE blob_fabric_shortcuts
                SET files_zone_path = ?, shortcut_name = ?, lakehouse_uri = ?, created_ts = ?, method = ?
                WHERE id = ?
                """,
                (files_zone_path, shortcut_name, lakehouse_uri, now, method, row[0])
            )
        else:
            conn.execute(
                """
                INSERT INTO blob_fabric_shortcuts 
                (session_id, blob_path, files_zone_path, shortcut_name, lakehouse_uri, created_ts, method)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (sid, blob_path, files_zone_path, shortcut_name, lakehouse_uri, now, method)
            )
        conn.commit()
        logger.info(f"Registered Fabric shortcut for blob '{blob_path}' -> '{files_zone_path}' under session '{sid}'.")
    finally:
        conn.close()

def get_shortcut(session_id: str, blob_path: str) -> Optional[Dict[str, Any]]:
    """
    Get the shortcut details for a specific blob path in a session.
    """
    sid = (session_id or "default").strip() or "default"
    # Match exact or prefixed (supporting both direct path or azure_blob: path)
    clean_path = blob_path.replace("azure_blob:", "")
    
    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT files_zone_path, shortcut_name, lakehouse_uri, created_ts, method
            FROM blob_fabric_shortcuts
            WHERE session_id = ? AND (blob_path = ? OR blob_path = ?)
            """,
            (sid, clean_path, f"azure_blob:{clean_path}")
        ).fetchone()
        
        if not row:
            return None
            
        return {
            "files_zone_path": row[0],
            "shortcut_name": row[1],
            "lakehouse_uri": row[2],
            "created_ts": row[3],
            "method": row[4]
        }
    finally:
        conn.close()

def list_shortcuts(session_id: str) -> List[Dict[str, Any]]:
    """
    List all shortcuts registered under a session.
    """
    sid = (session_id or "default").strip() or "default"
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT blob_path, files_zone_path, shortcut_name, lakehouse_uri, created_ts, method
            FROM blob_fabric_shortcuts
            WHERE session_id = ?
            ORDER BY created_ts DESC
            """,
            (sid,)
        ).fetchall()
        
        return [
            {
                "blob_path": r[0],
                "files_zone_path": r[1],
                "shortcut_name": r[2],
                "lakehouse_uri": r[3],
                "created_ts": r[4],
                "method": r[5]
            }
            for r in rows
        ]
    finally:
        conn.close()
