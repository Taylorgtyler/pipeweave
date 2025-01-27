from typing import Callable, List, Dict, Optional, Union, Set, Any, TypeVar
from dataclasses import dataclass, field
from enum import Enum


class State(Enum):
    """Enumeration of possible states for Steps, Stages, and Pipelines.

    The State enum represents the different states that a pipeline component can be in
    during execution. These states help track the progress and status of the pipeline.

    Attributes:
        IDLE: Initial state, ready to execute.
        RUNNING: Currently executing.
        COMPLETED: Successfully finished execution.
        ERROR: Failed during execution.
    """

    IDLE = "IDLE"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"


T = TypeVar('T')  # Type variable for input data
R = TypeVar('R')  # Type variable for output data

@dataclass
class Step:
    """A step in a data processing pipeline.

    A Step represents a single unit of work in a pipeline. It encapsulates a function
    that processes data, along with metadata about its inputs, outputs, and dependencies.
    Steps can be connected together to form a pipeline, with data flowing between them
    based on their input/output specifications.

    Attributes:
        name (str): Unique identifier for the step.
        description (str): Human-readable description of the step's purpose.
        function (Callable): The function to execute for this step.
        inputs (List[str]): List of input names expected by the function.
        outputs (List[str]): List of output names produced by the function.
        dependencies (Set[str]): Set of step names that must execute before this step.
        state (State): Current execution state of the step.

    Example:
        >>> def double_number(x: int) -> int:
        ...     return x * 2
        >>> step = Step(
        ...     name="double",
        ...     description="Double the input number",
        ...     function=double_number,
        ...     inputs=["number"],
        ...     outputs=["result"],
        ...     dependencies=set()
        ... )
        >>> result = step.execute({"number": 5})
        >>> print(result["result"])
        10
    """

    name: str
    description: str
    function: Callable[[T], R]
    inputs: List[str]
    outputs: List[str]
    dependencies: Set[str] = field(default_factory=set)
    state: State = State.IDLE

    def execute(self, data: Union[T, Dict[str, T]]) -> Union[R, Dict[str, R]]:
        """Execute the step with the provided input data.

        This method runs the step's function with the provided input data and returns
        the results. It also manages the step's state during execution.

        Args:
            data (Union[T, Dict[str, T]]): Input data for the function.
                Can be either a single value or a dictionary mapping input names to values.

        Returns:
            Union[R, Dict[str, R]]: The function's output.
                If the function returns a dictionary, it is returned as is.
                Otherwise, the output is wrapped in a dictionary using the first output name.

        Raises:
            Exception: Any exception raised by the function is propagated up.
                The step's state is set to ERROR in this case.

        Example:
            >>> step = Step("double", "Double input", lambda x: x * 2, ["num"], ["result"])
            >>> result = step.execute({"num": 5})
            >>> print(result["result"])
            10
        """
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
        """Reset the step to its initial state.

        This method resets the step's state back to IDLE, allowing it to be executed again.
        This is typically called when resetting the entire pipeline.

        Example:
            >>> step.execute({"num": 5})
            >>> step.reset()
            >>> step.execute({"num": 10})  # Execute again with different input
        """
        self.state = State.IDLE
