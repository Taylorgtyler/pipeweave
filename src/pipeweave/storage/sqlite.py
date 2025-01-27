"""SQLite storage backend for pipelines."""

import sqlite3
import json
import pickle
from typing import Dict, Any, Optional, TYPE_CHECKING
from contextlib import contextmanager
from .base import StorageBackend
from datetime import datetime
from pathlib import Path

if TYPE_CHECKING:
    from ..core import Pipeline


class SQLiteStorage(StorageBackend):
    """SQLite storage backend for pipelines.

    This class implements pipeline storage using SQLite as the backend database.
    It handles serialization and deserialization of pipeline objects, and provides
    CRUD operations for pipeline management.

    Attributes:
        db_path (Path): Path to the SQLite database file
    """

    def __init__(self, db_path: str):
        """Initialize SQLite storage.

        Args:
            db_path (str): Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self._init_db()

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        """Initialize the database schema if it doesn't exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pipelines (
                    name TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    steps TEXT NOT NULL,
                    stages TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS steps (
                    name TEXT,
                    pipeline_name TEXT,
                    description TEXT,
                    function BLOB,
                    inputs TEXT,
                    outputs TEXT,
                    dependencies TEXT,
                    state TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (name, pipeline_name),
                    FOREIGN KEY (pipeline_name) REFERENCES pipelines(name) ON DELETE CASCADE
                )
            """
            )

            conn.commit()

    def save_pipeline(self, pipeline: "Pipeline") -> None:
        """Save a pipeline to the database.

        Args:
            pipeline (Pipeline): Pipeline instance to save

        Raises:
            sqlite3.Error: If there's a database error
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Convert steps and stages to JSON
            steps_json = json.dumps(
                {
                    name: {
                        "description": step.description,
                        "inputs": step.inputs,
                        "outputs": step.outputs,
                        "dependencies": list(step.dependencies),
                        "state": step.state.name,
                    }
                    for name, step in pipeline.steps.items()
                }
            )

            stages_json = json.dumps(
                {
                    name: {
                        "description": stage.description,
                        "steps": [step.name for step in stage.steps],
                        "dependencies": list(stage.dependencies),
                        "state": stage.state.name,
                    }
                    for name, stage in pipeline.stages.items()
                }
            )

            # Update or insert pipeline
            cursor.execute(
                """
                INSERT OR REPLACE INTO pipelines (
                    name, state, steps, stages, updated_at
                ) VALUES (?, ?, ?, ?, ?)
            """,
                (
                    pipeline.name,
                    pipeline.state.name,
                    steps_json,
                    stages_json,
                    datetime.now().isoformat(),
                ),
            )

            conn.commit()

    def load_pipeline(self, name: str) -> "Pipeline":
        """Load a pipeline from the database.

        Args:
            name (str): Name of pipeline to load

        Returns:
            Pipeline: Loaded pipeline instance

        Raises:
            ValueError: If pipeline not found
            sqlite3.Error: If there's a database error
        """
        # Import here to avoid circular imports
        from ..core import Pipeline, create_step, create_stage
        from ..step import State

        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                "SELECT state, steps, stages FROM pipelines WHERE name = ?", (name,)
            )
            row = cursor.fetchone()

            if not row:
                raise ValueError(f"Pipeline '{name}' not found")

            state_name, steps_json, stages_json = row

            # Create pipeline instance
            pipeline = Pipeline(name=name)
            pipeline.state = State[state_name]

            # Load steps
            steps_data = json.loads(steps_json)
            for step_name, step_info in steps_data.items():
                step = create_step(
                    name=step_name,
                    description=step_info["description"],
                    function=lambda x: x,  # Placeholder function
                    inputs=step_info["inputs"],
                    outputs=step_info["outputs"],
                    dependencies=set(step_info["dependencies"]),
                )
                step.state = State[step_info["state"]]
                pipeline.steps[step_name] = step

            # Load stages
            stages_data = json.loads(stages_json)
            for stage_name, stage_info in stages_data.items():
                stage_steps = [
                    pipeline.steps[step_name] for step_name in stage_info["steps"]
                ]
                stage = create_stage(
                    name=stage_name,
                    description=stage_info["description"],
                    steps=stage_steps,
                    dependencies=set(stage_info["dependencies"]),
                )
                stage.state = State[stage_info["state"]]
                pipeline.stages[stage_name] = stage

            return pipeline

    def delete_pipeline(self, name: str) -> None:
        """Delete a pipeline from the database.

        Args:
            name (str): Name of pipeline to delete

        Raises:
            ValueError: If pipeline not found
            sqlite3.Error: If there's a database error
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("DELETE FROM pipelines WHERE name = ?", (name,))
            if cursor.rowcount == 0:
                raise ValueError(f"Pipeline '{name}' not found")

            conn.commit()

    def list_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """List all pipelines in the database.

        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping pipeline names to their metadata

        Raises:
            sqlite3.Error: If there's a database error
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT name, state, created_at, updated_at FROM pipelines")
            return {
                row[0]: {"state": row[1], "created_at": row[2], "updated_at": row[3]}
                for row in cursor.fetchall()
            }
