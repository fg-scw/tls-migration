"""Migration state persistence for resume/rollback support."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from vmware2scw.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class MigrationState:
    """Persistent state for a single migration.

    Stored as JSON in the work directory. Enables resume after failure
    by tracking completed stages and intermediate artifacts.
    """
    migration_id: str
    vm_name: str
    target_type: str = ""
    zone: str = "fr-par-1"
    current_stage: str = ""
    completed_stages: list[str] = field(default_factory=list)
    artifacts: dict[str, Any] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        if d["started_at"]:
            d["started_at"] = d["started_at"].isoformat()
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "MigrationState":
        if data.get("started_at") and isinstance(data["started_at"], str):
            data["started_at"] = datetime.fromisoformat(data["started_at"])
        return cls(**data)


class MigrationStateStore:
    """Persists migration state to disk as JSON files.

    State files are stored at: {work_dir}/state/{migration_id}.json

    Confidence: 85 — Simple file-based persistence. Adequate for CLI tool.
    For multi-user/concurrent use, would need a proper database.
    """

    def __init__(self, work_dir: Path | str = "/var/lib/vmware2scw/work"):
        self.state_dir = Path(work_dir) / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _state_path(self, migration_id: str) -> Path:
        return self.state_dir / f"{migration_id}.json"

    def save(self, state: MigrationState) -> None:
        """Save migration state to disk."""
        path = self._state_path(state.migration_id)
        with open(path, "w") as f:
            json.dump(state.to_dict(), f, indent=2, default=str)

    def load(self, migration_id: str) -> Optional[MigrationState]:
        """Load migration state from disk."""
        path = self._state_path(migration_id)
        if not path.exists():
            return None
        try:
            with open(path) as f:
                data = json.load(f)
            return MigrationState.from_dict(data)
        except Exception as e:
            logger.error(f"Failed to load state for {migration_id}: {e}")
            return None

    def list_all(self) -> list[MigrationState]:
        """List all known migration states."""
        states = []
        for path in self.state_dir.glob("*.json"):
            state = self.load(path.stem)
            if state:
                states.append(state)
        return sorted(states, key=lambda s: s.started_at or datetime.min, reverse=True)

    def delete(self, migration_id: str) -> None:
        """Delete a migration state file."""
        path = self._state_path(migration_id)
        if path.exists():
            path.unlink()
