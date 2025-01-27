from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Set, Union, TypeVar, Iterator
import logging
from .step import Step, State
from .stage import Stage
from .storage.base import StorageBackend

T = TypeVar("T")  # Type variable for generic input/output types


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
    """A pipeline that executes a sequence of steps and stages in dependency order.

    A pipeline represents a workflow where data flows through a sequence of steps and stages.
    Each step or stage receives input from the previous step/stage's output, creating a natural
    data transformation flow. Steps within a stage are executed sequentially, with each step's
    output feeding into the next step.

    For independent operations that need to work with the original input data, it's recommended to:
    1. Use stages with single steps
    2. Use explicit dependencies (when supported)
    3. Create separate pipelines for independent operations

    Attributes:
        name (str): Name of the pipeline.
        description (str): Description of what the pipeline does.
        stages (Dict[str, Stage]): Dictionary of stages in the pipeline.
        steps (Dict[str, Step]): Dictionary of steps in the pipeline.
        results (Dict[str, Dict[str, Any]]): Results from pipeline execution.
        state (State): Current state of the pipeline.
        current_stage (Optional[Stage]): Currently executing stage.
        current_step (Optional[Step]): Currently executing step.
        logger (logging.Logger): Logger for pipeline events.

    Example:
        >>> pipeline = Pipeline("math_ops")
        >>> pipeline.add_stage(Stage("stage1", "First stage", [
        ...     Step("double", "Double input", lambda x: x * 2, ["num"], ["result"])
        ... ]))
        >>> pipeline.add_step(Step("add_one", "Add one", lambda x: x + 1, ["result"], ["final"]))
        >>> results = pipeline.run(5)  # Input flows: 5 -> double (10) -> add_one (11)
        >>> print(results["double"]["result"])  # 10
        >>> print(results["add_one"]["final"])  # 11
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

        This method executes all steps and stages in the pipeline in dependency order.
        Data flows through the pipeline sequentially, where each step/stage receives
        the output from the previous step/stage as its input. This creates a natural
        transformation pipeline where data is processed in stages.

        For example, if you have:
        - A stage with a step that doubles a number
        - A step that adds one
        The data will flow: input (5) -> double (10) -> add_one (11)

        Args:
            input_data (Optional[T], optional): Initial input data to pass to the pipeline.
                This data will be passed to the first step/stage. Defaults to None.
                If a raw value is provided, it will be wrapped in a dictionary using the first
                input name of the first step/stage.

        Returns:
            Dict[str, Dict[str, Any]]: Dictionary containing the results of all pipeline steps,
                organized as {step_name: {output_name: value}}.

        Raises:
            ValueError: If a circular dependency is detected in the pipeline.
            Exception: If any step fails during execution, the original exception is re-raised.

        Example:
            >>> pipeline = Pipeline("math_ops")
            >>> pipeline.add_stage(Stage("stage1", "First stage", [
            ...     Step("double", "Double input", lambda x: x * 2, ["num"], ["result"])
            ... ]))
            >>> pipeline.add_step(Step("add_one", "Add one", lambda x: x + 1, ["result"], ["final"]))
            >>> results = pipeline.run(5)  # Input flows: 5 -> double (10) -> add_one (11)
            >>> print(results["double"]["result"])  # 10
            >>> print(results["add_one"]["final"])  # 11
        """
        self.state = State.RUNNING
        current_data: Any = input_data

        try:
            ordered_items = self._get_execution_order()

            # If input is not None and not a dict, wrap it in a dict using the first input name
            if current_data is not None and not isinstance(current_data, dict):
                # Find first step/stage to get input name
                first_item = ordered_items[0]
                if isinstance(first_item, Stage):
                    input_name = first_item.steps[0].inputs[0]
                else:
                    input_name = first_item.inputs[0]
                current_data = {input_name: current_data}

            for item in ordered_items:
                if isinstance(item, Stage):
                    self.current_stage = item
                    self.logger.info(f"Executing stage: {item.name}")
                    stage_results = item.execute(current_data)
                    self.results.update(stage_results)
                    # Pass the last step's result to the next item
                    last_step = item.steps[-1]
                    current_data = self.results[last_step.name]
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
                    else:
                        # If no dependencies, use current data
                        if current_data is not None:
                            if isinstance(current_data, dict):
                                # Map outputs to inputs if names don't match
                                if len(item.inputs) == 1 and not any(
                                    k in item.inputs for k in current_data
                                ):
                                    dep_data = {
                                        item.inputs[0]: next(
                                            iter(current_data.values())
                                        )
                                    }
                                else:
                                    dep_data = current_data
                            elif len(item.inputs) == 1:
                                dep_data = {item.inputs[0]: current_data}

                    result = item.execute(dep_data)

                    # Store results with output names
                    if isinstance(result, dict):
                        self.results[item.name] = result
                    else:
                        self.results[item.name] = {item.outputs[0]: result}

                    # Update current_data for next item
                    current_data = self.results[item.name]

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
