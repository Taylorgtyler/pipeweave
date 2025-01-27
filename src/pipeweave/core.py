from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Set, Union, TypeVar, Iterator
import logging
from .step import Step, State
from .stage import Stage
from .storage.base import StorageBackend

T = TypeVar('T')  # Type variable for generic input/output types

def create_step(
    name: str,
    description: str,
    function: Callable[[T], Any],
    inputs: List[str],
    outputs: List[str],
    dependencies: Optional[Set[str]] = None,
) -> Step:
    """Create a new Step instance.

    Args:
        name (str): Unique identifier for the step.
        description (str): Human-readable description of the step's purpose.
        function (Callable[[T], Any]): The function to execute for this step.
            The function should take a single argument of type T and return any type.
        inputs (List[str]): List of input names expected by the function.
            These names are used to map data from previous steps.
        outputs (List[str]): List of output names produced by the function.
            These names are used to map data to subsequent steps.
        dependencies (Optional[Set[str]], optional): Set of step names that must execute before this step.
            Dependencies are used to determine execution order. Defaults to None.

    Returns:
        Step: A new Step instance configured with the provided parameters.

    Raises:
        ValueError: If dependencies contain step names that don't exist in the pipeline.
    """
    step = Step(
        name=name,
        description=description,
        function=function,
        inputs=inputs,
        outputs=outputs,
        dependencies=dependencies or set(),
    )
    return step


def create_stage(
    name: str,
    description: str,
    steps: List[Step],
    dependencies: Optional[Set[str]] = None,
) -> Stage:
    """Create a new Stage instance.

    A stage is a collection of steps that are logically grouped together and can be
    executed as a unit. Stages can have dependencies on other stages, ensuring proper
    execution order in the pipeline.

    Args:
        name (str): Unique identifier for the stage.
        description (str): Human-readable description of the stage's purpose.
        steps (List[Step]): List of steps to be included in this stage.
            Steps within a stage are executed in dependency order.
        dependencies (Optional[Set[str]], optional): Set of stage names that must execute before this stage.
            Used to determine execution order between stages. Defaults to None.

    Returns:
        Stage: A new Stage instance configured with the provided parameters.

    Raises:
        ValueError: If dependencies contain stage names that don't exist in the pipeline.
    """
    stage = Stage(
        name=name,
        description=description,
        steps=steps,
        dependencies=dependencies or set(),
    )
    return stage


class Pipeline:
    """A generic pipeline class that manages the execution of data processing steps.

    The Pipeline class provides functionality to create and execute data processing pipelines
    by managing a series of interconnected steps and stages. It handles dependency resolution,
    execution order, and maintains state throughout the pipeline's lifecycle.

    Attributes:
        name (str): The unique identifier of the pipeline.
        steps (Dict[str, Step]): Dictionary mapping step names to Step objects.
        stages (Dict[str, Stage]): Dictionary mapping stage names to Stage objects.
        state (State): Current state of the pipeline (IDLE, RUNNING, COMPLETED, ERROR).
        current_step (Optional[Step]): Reference to the currently executing step.
        current_stage (Optional[Stage]): Reference to the currently executing stage.
        results (Dict[str, Dict[str, Any]]): Dictionary storing the results of each step.
            Organized as {step_name: {output_name: value}}.
        logger (logging.Logger): Logger instance for pipeline execution logs.

    Example:
        >>> pipeline = Pipeline("data_transformer")
        >>> step1 = create_step("double", "Double input", lambda x: x * 2, ["num"], ["result"])
        >>> pipeline.add_step(step1)
        >>> results = pipeline.run(5)
        >>> print(results["double"]["result"])
        10
    """

    def __init__(self, name: str) -> None:
        """Initialize a new Pipeline instance.

        Args:
            name (str): The unique identifier for the pipeline.
        """
        self.name: str = name
        self.steps: Dict[str, Step] = {}
        self.stages: Dict[str, Stage] = {}
        self.state: State = State.IDLE
        self.current_step: Optional[Step] = None
        self.current_stage: Optional[Stage] = None
        self.results: Dict[str, Dict[str, Any]] = {}
        self.logger: logging.Logger = logging.getLogger(__name__)

    def add_step(self, step: Step) -> Pipeline:
        """Add a step to the pipeline.

        This method adds a step to the pipeline and validates its dependencies.
        Steps can be added individually or as part of a stage.

        Args:
            step (Step): The step to add to the pipeline.
                The step must have a unique name within the pipeline.

        Returns:
            Pipeline: The pipeline instance for method chaining.

        Raises:
            ValueError: If a step with the same name already exists or if a dependency
                is specified that doesn't exist in the pipeline.

        Example:
            >>> step = create_step("double", "Double input", lambda x: x * 2, ["num"], ["result"])
            >>> pipeline.add_step(step)
        """
        # Validate dependencies exist
        for dep in step.dependencies:
            if dep not in self.steps:
                raise ValueError(f"Dependency '{dep}' not found in pipeline steps")

        self.steps[step.name] = step
        return self

    def add_stage(self, stage: Stage) -> Pipeline:
        """Add a stage to the pipeline.

        This method adds a stage and all its steps to the pipeline. It validates both
        stage-level and step-level dependencies.

        Args:
            stage (Stage): The stage to add to the pipeline.
                The stage and all its steps must have unique names within the pipeline.

        Returns:
            Pipeline: The pipeline instance for method chaining.

        Raises:
            ValueError: If a stage with the same name already exists, or if any stage
                or step dependencies are not found in the pipeline.

        Example:
            >>> steps = [create_step("step1", "Step 1", fn1, ["in1"], ["out1"]),
            ...         create_step("step2", "Step 2", fn2, ["out1"], ["out2"])]
            >>> stage = create_stage("process", "Processing stage", steps)
            >>> pipeline.add_stage(stage)
        """
        # Validate dependencies exist
        for dep in stage.dependencies:
            if dep not in self.stages:
                raise ValueError(f"Stage dependency '{dep}' not found in pipeline")

        self.stages[stage.name] = stage

        # Add all steps from the stage to the pipeline's steps
        for step in stage.steps:
            self.steps[step.name] = step

        return self

    def _get_execution_order(self) -> List[Union[Step, Stage]]:
        """Determine the correct execution order based on dependencies.

        This method performs a topological sort of the pipeline's stages and steps
        based on their dependencies to determine the correct execution order.

        Returns:
            List[Union[Step, Stage]]: A list of stages and steps in the order they
                should be executed.

        Raises:
            ValueError: If a circular dependency is detected in the pipeline's stages
                or steps.
        """
        executed: Set[str] = set()
        execution_order: List[Union[Step, Stage]] = []

        def can_execute(item: Union[Step, Stage]) -> bool:
            """Check if all dependencies for a step or stage have been executed."""
            return all(dep in executed for dep in item.dependencies)

        # First handle stages
        while len(executed) < len(self.stages):
            ready_stages = [
                stage
                for name, stage in self.stages.items()
                if name not in executed and can_execute(stage)
            ]

            if not ready_stages and len(executed) < len(self.stages):
                raise ValueError("Circular dependency detected in pipeline stages")

            execution_order.extend(ready_stages)
            executed.update(stage.name for stage in ready_stages)

        # Then handle any remaining individual steps
        remaining_steps = [
            step
            for name, step in self.steps.items()
            if not any(step in stage.steps for stage in self.stages.values())
        ]

        executed.clear()
        while remaining_steps:
            ready_steps = [
                step
                for step in remaining_steps
                if step.name not in executed and can_execute(step)
            ]

            if not ready_steps and remaining_steps:
                raise ValueError("Circular dependency detected in pipeline steps")

            execution_order.extend(ready_steps)
            executed.update(step.name for step in ready_steps)
            remaining_steps = [s for s in remaining_steps if s not in ready_steps]

        return execution_order

    def run(self, input_data: Optional[T] = None) -> Dict[str, Dict[str, Any]]:
        """Execute the pipeline with the given input data.

        This method executes all steps and stages in the pipeline in dependency order,
        passing data between steps and handling any errors that occur during execution.

        Args:
            input_data (Optional[T], optional): Initial input data to pass to the pipeline.
                This data will be passed to steps that don't have dependencies. Defaults to None.

        Returns:
            Dict[str, Dict[str, Any]]: Dictionary containing the results of all pipeline steps,
                organized as {step_name: {output_name: value}}.

        Raises:
            ValueError: If a circular dependency is detected in the pipeline.
            Exception: If any step fails during execution, the original exception is re-raised.

        Example:
            >>> pipeline = Pipeline("math_ops")
            >>> pipeline.add_step(create_step("double", "Double input", lambda x: x * 2, ["num"], ["result"]))
            >>> results = pipeline.run(5)
            >>> print(results["double"]["result"])
            10
        """
        self.state = State.RUNNING
        current_data: Any = input_data

        try:
            ordered_items = self._get_execution_order()

            for item in ordered_items:
                if isinstance(item, Stage):
                    self.current_stage = item
                    self.logger.info(f"Executing stage: {item.name}")
                    stage_results = item.execute(current_data)
                    self.results.update(stage_results)
                    current_data = stage_results
                else:  # Step
                    self.current_step = item
                    self.logger.info(f"Executing step: {item.name}")

                    # Prepare input data based on dependencies and inputs
                    dep_data: Dict[str, Any] = {}

                    if item.dependencies:
                        dep_data = {
                            output: self.results[dep][output]
                            for dep in item.dependencies
                            for output in self.steps[dep].outputs
                            if output in item.inputs
                        }
                        if len(item.inputs) == 1 and item.inputs[0] in dep_data:
                            dep_data = {item.inputs[0]: dep_data[item.inputs[0]]}

                    if current_data is not None:
                        input_dict = (
                            {item.inputs[0]: current_data}
                            if isinstance(current_data, (int, float, str))
                            else current_data
                        )
                        dep_data.update(input_dict)

                    current_data = item.execute(dep_data)

                    # Store results with output names
                    if isinstance(current_data, dict):
                        self.results[item.name] = current_data
                    else:
                        self.results[item.name] = {item.outputs[0]: current_data}

            self.state = State.COMPLETED
            return self.results

        except Exception as e:
            self.state = State.ERROR
            if self.current_stage:
                self.current_stage.state = State.ERROR
            if self.current_step:
                self.current_step.state = State.ERROR
            self.logger.error(
                f"Pipeline error in {'stage' if self.current_stage else 'step'} "
                f"{self.current_stage.name if self.current_stage else self.current_step.name}: {str(e)}"
            )
            raise

    def reset(self) -> None:
        """Reset the pipeline to its initial state.

        This method clears all results and resets the state of the pipeline and all its
        components (steps and stages) back to IDLE, allowing the pipeline to be run again.

        Example:
            >>> pipeline.run(5)
            >>> pipeline.reset()
            >>> pipeline.run(10)  # Run again with different input
        """
        self.state = State.IDLE
        self.current_step = None
        self.current_stage = None
        self.results.clear()
        for step in self.steps.values():
            step.reset()
        for stage in self.stages.values():
            stage.reset()

    def save(self, storage: StorageBackend) -> None:
        """Save pipeline to storage backend.

        This method persists the pipeline's configuration and state to the specified
        storage backend for later retrieval.

        Args:
            storage (StorageBackend): Storage backend to save to.
                The backend must implement the StorageBackend interface.

        Example:
            >>> from pipeweave.storage import SQLiteStorage
            >>> storage = SQLiteStorage("pipelines.db")
            >>> pipeline.save(storage)
        """
        storage.save_pipeline(self)

    @classmethod
    def load(cls, storage: StorageBackend, pipeline_name: str) -> Pipeline:
        """Load pipeline from storage backend.

        This class method retrieves a previously saved pipeline from the specified
        storage backend.

        Args:
            storage (StorageBackend): Storage backend to load from.
                The backend must implement the StorageBackend interface.
            pipeline_name (str): Name of pipeline to load.

        Returns:
            Pipeline: Loaded pipeline instance.

        Example:
            >>> from pipeweave.storage import SQLiteStorage
            >>> storage = SQLiteStorage("pipelines.db")
            >>> pipeline = Pipeline.load(storage, "my_pipeline")
        """
        return storage.load_pipeline(pipeline_name)
