from __future__ import annotations
import pytest

from agent.jobs_store import save_checkpoint, load_checkpoint
from agent.errors import AgentError, AgentErrorCode


def test_checkpoint_save_and_load():
    """
    Contract: Checkpoint data saved under a job_id and stage must be loadable
    exactly as saved, and return None if the checkpoint does not exist.
    """
    job_id = "test_job_123"
    stage = "source_loader"
    stage_output = {"ok": True, "datasets_count": 5}
    
    # Save checkpoint
    save_checkpoint(job_id, stage, stage_output)
    
    # Load checkpoint
    loaded = load_checkpoint(job_id, stage)
    assert loaded == stage_output
    
    # Load non-existent
    assert load_checkpoint(job_id, "non_existent") is None


def test_agent_error_taxonomy():
    """
    Contract: AgentError must wrap AgentErrorCode, message, and recoverable flag correctly,
    and be serializable via to_dict().
    """
    err = AgentError(AgentErrorCode.CONNECTION_FAILED, "Could not connect to database", recoverable=True)
    assert err.code == AgentErrorCode.CONNECTION_FAILED
    assert err.message == "Could not connect to database"
    assert err.recoverable is True
    
    d = err.to_dict()
    assert d["code"] == "CONNECTION_FAILED"
    assert d["message"] == "Could not connect to database"
    assert d["recoverable"] is True
