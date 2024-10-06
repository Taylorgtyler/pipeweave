from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Union
from enum import Enum
import logging

class State(Enum):
    """Enumeration of possible states for Steps and Pipelines."""
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"

