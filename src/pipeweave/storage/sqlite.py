import sqlite3
import json
import pickle
from typing import Dict, Any
from contextlib import contextmanager
from .base import StorageBackend
from ..core import Pipeline
from ..step import State


class SQLiteStorage(StorageBackend):
    """SQLite implementation of pipeline storage."""

    def __init__(self, db_path: str):
        """Initialize SQLite storage.

        Args:
            db_path (str): Path to SQLite database file
        """
        self.db_path = db_path
        self._initialize_db()

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def _initialize_db(self) -> None:
        """Create database tables if they don't exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pipelines (
                    name TEXT PRIMARY KEY,
                    state TEXT,
                    results TEXT,
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

    def save_pipeline(self, pipeline: Pipeline) -> None:
        """Save pipeline to SQLite database."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Save pipeline
            cursor.execute(
                """INSERT OR REPLACE INTO pipelines (name, state, results, updated_at) 
                   VALUES (?, ?, ?, CURRENT_TIMESTAMP)""",
                (pipeline.name, pipeline.state.name, json.dumps(pipeline.results)),
            )

            # Save steps
            for step in pipeline.steps.values():
                cursor.execute(
                    """INSERT OR REPLACE INTO steps 
                       (name, pipeline_name, description, function, inputs, outputs, 
                        dependencies, state)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        step.name,
                        pipeline.name,
                        step.description,
                        pickle.dumps(step.function),
                        json.dumps(step.inputs),
                        json.dumps(step.outputs),
                        json.dumps(list(step.dependencies)),
                        step.state.name,
                    ),
                )

            conn.commit()

    def load_pipeline(self, pipeline_name: str) -> Pipeline:
        """Load pipeline from SQLite database."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Load pipeline
            cursor.execute("SELECT * FROM pipelines WHERE name = ?", (pipeline_name,))
            pipeline_data = cursor.fetchone()

            if not pipeline_data:
                raise ValueError(f"Pipeline '{pipeline_name}' not found in database")

            pipeline = Pipeline(pipeline_name)
            pipeline.state = State[pipeline_data[1]]
            pipeline.results = json.loads(pipeline_data[2])

            # Load steps
            cursor.execute(
                "SELECT * FROM steps WHERE pipeline_name = ?", (pipeline_name,)
            )
            for step_data in cursor.fetchall():
                pipeline.add_step(
                    name=step_data[0],
                    description=step_data[2],
                    function=pickle.loads(step_data[3]),
                    inputs=json.loads(step_data[4]),
                    outputs=json.loads(step_data[5]),
                    dependencies=set(json.loads(step_data[6])),
                )
                pipeline.steps[step_data[0]].state = State[step_data[7]]

            return pipeline

    def delete_pipeline(self, pipeline_name: str) -> None:
        """Delete pipeline from SQLite database."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("DELETE FROM pipelines WHERE name = ?", (pipeline_name,))

            if cursor.rowcount == 0:
                raise ValueError(f"Pipeline '{pipeline_name}' not found in database")

            conn.commit()

    def list_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """List all pipelines in the database."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT name, state, created_at, updated_at 
                FROM pipelines
            """
            )

            return {
                row[0]: {"state": row[1], "created_at": row[2], "updated_at": row[3]}
                for row in cursor.fetchall()
            }
