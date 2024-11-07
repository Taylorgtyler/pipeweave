from typing import Callable, List, Dict, Optional, Union, Set, Any
from dataclasses import dataclass, field
from enum import Enum


class State(Enum):
    """Enumeration of possible states for Steps and Pipelines."""

    IDLE = "IDLE"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"


@dataclass
class Step:
    """A step in a pipeline."""

    name: str
    description: str
    function: Callable
    inputs: List[str]
    outputs: List[str]
    dependencies: Set[str] = field(default_factory=set)
    state: State = State.IDLE

    def execute(self, data: Any) -> Any:
        """Execute the step with the provided input data."""
        try:
            self.state = State.RUNNING
            # Extract the value if it's a dictionary with a single input
            if isinstance(data, dict) and len(self.inputs) == 1:
                data = data[self.inputs[0]]
            result = self.function(data)
            self.state = State.COMPLETED
            return result
        except Exception as e:
            self.state = State.ERROR
            raise

    def reset(self) -> None:
        """Reset the step to its initial state."""
        self.state = State.IDLE
