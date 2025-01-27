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


T = TypeVar("T")  # Type variable for input data
R = TypeVar("R")  # Type variable for output data


@dataclass
class Step:
    """A single step in a data processing pipeline.

    A step represents a single data transformation operation in a pipeline.
    When part of a stage or pipeline, a step typically receives input from
    the previous step's output, creating a natural data transformation flow.

    For independent operations that need to work with the original input data,
    it's recommended to:
    1. Place the step in its own stage
    2. Use explicit dependencies (when supported)
    3. Create a separate pipeline

    Attributes:
        name (str): Name of the step.
        description (str): Description of what the step does.
        function (Callable): Function that implements the step's logic.
        inputs (List[str]): List of input names the function expects.
        outputs (List[str]): List of output names the function produces.
        dependencies (Set[str]): Set of step names that must execute before this step.
        state (State): Current state of the step.

    Example:
        >>> step = Step("double", "Double input", lambda x: x * 2, ["num"], ["result"])
        >>> result = step.execute({"num": 5})
        >>> print(result["result"])  # 10
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

        This method runs the step's function with the provided input data.
        When part of a stage or pipeline, the input data typically comes
        from the previous step's output, creating a natural data flow.

        Args:
            data (Union[T, Dict[str, T]]): Input data for the function.
                Can be either a single value or a dictionary mapping input names to values.
                If a dictionary is provided and there's only one input, the value is extracted.
                In a pipeline, this is typically the output from the previous step.

        Returns:
            Union[R, Dict[str, R]]: The function's output.
                If the function returns a dictionary, it is returned as is.
                Otherwise, the output is wrapped in a dictionary using the first output name.
                This output will typically be passed to the next step in the pipeline.

        Raises:
            Exception: Any exception raised by the function is propagated up.
                The step's state is set to ERROR in this case.

        Example:
            >>> step = Step("double", "Double input", lambda x: x * 2, ["num"], ["result"])
            >>> result = step.execute({"num": 5})  # In a pipeline, this might be output from previous step
            >>> print(result["result"])  # 10 (will be passed to next step)
        """
        try:
            self.state = State.RUNNING

            # Extract input value if it's a dictionary with a single input
            if isinstance(data, dict):
                if len(self.inputs) == 1:
                    data = data[self.inputs[0]]
                else:
                    # If multiple inputs, pass them as kwargs
                    data = {k: v for k, v in data.items() if k in self.inputs}

            # Execute function
            result = self.function(data)

            # Format output
            if isinstance(result, dict):
                output = result
            else:
                output = {self.outputs[0]: result}

            self.state = State.COMPLETED
            return output

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
