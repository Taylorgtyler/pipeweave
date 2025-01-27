from typing import List, Set, Any, Dict, TypeVar, Union, Optional
from dataclasses import dataclass, field

from .step import Step, State

T = TypeVar('T')  # Type variable for input data
R = TypeVar('R')  # Type variable for output data

@dataclass
class Stage:
    """A group of related steps in a data processing pipeline.

    A Stage represents a logical grouping of steps that should be executed together.
    Stages can have dependencies on other stages, allowing for complex pipeline
    structures with multiple levels of organization. Each stage maintains its own
    state and manages the execution of its contained steps.

    Attributes:
        name (str): Unique identifier for the stage.
        description (str): Human-readable description of the stage's purpose.
        steps (List[Step]): List of steps contained in this stage.
        dependencies (Set[str]): Set of stage names that must execute before this stage.
        state (State): Current execution state of the stage.

    Example:
        >>> def step1_fn(x: int) -> int:
        ...     return x * 2
        >>> def step2_fn(x: int) -> int:
        ...     return x + 1
        >>> step1 = Step("double", "Double input", step1_fn, ["num"], ["doubled"])
        >>> step2 = Step("add_one", "Add one", step2_fn, ["doubled"], ["result"])
        >>> stage = Stage(
        ...     name="process_numbers",
        ...     description="Process numbers with multiple operations",
        ...     steps=[step1, step2],
        ...     dependencies=set()
        ... )
        >>> results = stage.execute(5)
        >>> print(results["result"])
        11
    """

    name: str
    description: str
    steps: List[Step]
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

        This method executes all steps in the stage in sequence, passing data between
        steps based on their input/output specifications. The stage's state is updated
        to reflect its execution status.

        Args:
            data (Optional[T], optional): Input data for the first step.
                If provided, this data will be passed to steps that don't have dependencies.
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
            >>> results = stage.execute(5)
            >>> print(results["double"]["result"])  # 10
            >>> print(results["add_one"]["final"])  # 11
        """
        try:
            self.state = State.RUNNING
            results: Dict[str, Dict[str, R]] = {}
            current_data = data

            for step in self.steps:
                # Prepare input data based on dependencies
                if step.dependencies:
                    dep_data = {
                        output: results[dep][output]
                        for dep in step.dependencies
                        for output in next(s for s in self.steps if s.name == dep).outputs
                        if output in step.inputs
                    }
                    if len(step.inputs) == 1 and step.inputs[0] in dep_data:
                        dep_data = {step.inputs[0]: dep_data[step.inputs[0]]}
                else:
                    dep_data = (
                        {step.inputs[0]: current_data}
                        if current_data is not None
                        else {}
                    )

                # Execute step and store results
                step_result = step.execute(dep_data)
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
