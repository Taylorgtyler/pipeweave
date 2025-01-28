from typing import List, Set, Any, Dict, TypeVar, Union, Optional
from dataclasses import dataclass, field
from enum import Enum

from .step import Step, State

T = TypeVar("T")  # Type variable for input data
R = TypeVar("R")  # Type variable for output data


class State(Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"


@dataclass
class Stage:
    """A group of steps that are executed sequentially as part of a pipeline.

    A stage represents a logical grouping of steps that are executed in sequence.
    Data flows through the steps in a stage sequentially, with each step receiving
    the output from the previous step as its input. This creates a natural data
    transformation flow within the stage.

    For independent operations that need to work with the original input data,
    it's recommended to use separate stages with single steps.

    Attributes:
        name (str): Name of the stage.
        description (str): Description of what the stage does.
        steps (List[Step]): List of steps to execute in sequence.
        dependencies (Set[str]): Set of stage names that must execute before this stage.
        state (State): Current state of the stage.

    Example:
        >>> stage = Stage("math_ops", "Math operations", [
        ...     Step("double", "Double input", lambda x: x * 2, ["num"], ["result"]),
        ...     Step("add_one", "Add one", lambda x: x + 1, ["result"], ["final"])
        ... ])
        >>> results = stage.execute(5)  # Input flows: 5 -> double (10) -> add_one (11)
        >>> print(results["double"]["result"])  # 10
        >>> print(results["add_one"]["final"])  # 11
    """

    name: str
    description: Optional[str] = None
    steps: List[Step] = field(default_factory=list)
    dependencies: Set[str] = field(default_factory=set)
    state: State = State.IDLE

    @property
    def inputs(self) -> List[str]:
        """Get all inputs required by steps in this stage."""
        return list({input_name for step in self.steps for input_name in step.inputs})

    @property
    def outputs(self) -> List[str]:
        """Get all outputs produced by steps in this stage."""
        return list(
            {output_name for step in self.steps for output_name in step.outputs}
        )

    def execute(self, data: Optional[T] = None) -> Dict[str, Dict[str, R]]:
        """Execute all steps in the stage with the provided input data.

        This method executes all steps in the stage in sequence. Data flows through
        the steps sequentially, where each step receives the output from the previous
        step as its input. This creates a natural transformation pipeline where data
        is processed step by step.

        For example, if you have:
        - A step that doubles a number
        - A step that adds one
        The data will flow: input (5) -> double (10) -> add_one (11)

        Args:
            data (Optional[T], optional): Input data for the first step.
                If provided, this data will be passed to the first step.
                Can be a raw value or a dictionary mapping input names to values.
                Raw values are automatically wrapped using the first step's first input name.
                Defaults to None.

        Returns:
            Dict[str, Dict[str, R]]: Dictionary containing the results of all steps,
                organized as {step_name: {output_name: value}}.

        Raises:
            Exception: Any exception raised by a step is propagated up.
                The stage's state is set to ERROR in this case.

        Example:
            >>> stage = Stage("math_ops", "Math operations", [
            ...     Step("double", "Double input", lambda x: x * 2, ["num"], ["result"]),
            ...     Step("add_one", "Add one", lambda x: x + 1, ["result"], ["final"])
            ... ])
            >>> results = stage.execute(5)  # Input flows: 5 -> double (10) -> add_one (11)
            >>> print(results["double"]["result"])  # 10
            >>> print(results["add_one"]["final"])  # 11
        """
        try:
            self.state = State.RUNNING
            results: Dict[str, Dict[str, R]] = {}
            current_data = data

            # If input is not None and not a dict, wrap it in a dict using first step's first input
            if current_data is not None and not isinstance(current_data, dict):
                current_data = {self.steps[0].inputs[0]: current_data}

            for step in self.steps:
                # Execute step and store results
                step_result = step.execute(current_data)
                if isinstance(step_result, dict):
                    results[step.name] = step_result
                else:
                    results[step.name] = {step.outputs[0]: step_result}

                # Update current_data for next step
                current_data = step_result

            self.state = State.COMPLETED
            return results

        except Exception as e:
            self.state = State.ERROR
            raise

    def reset(self) -> None:
        """Reset the stage and all its steps to their initial state.

        This method resets the stage's state and the state of all contained steps
        back to IDLE, allowing the stage to be executed again. This is typically
        called when resetting the entire pipeline.

        Example:
            >>> stage.execute(5)
            >>> stage.reset()
            >>> stage.execute(10)  # Execute again with different input
        """
        self.state = State.IDLE
        for step in self.steps:
            step.reset()
