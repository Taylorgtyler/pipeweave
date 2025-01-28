from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Set, Union, TypeVar, Iterator
import logging
from .step import Step, State
from .stage import Stage
from .storage import StorageBackend
from inspect import signature

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

    def __init__(self, name: str, description: Optional[str] = None):
        self.name = name
        self.description = description
        self.steps = []
        self.stages: Dict[str, Stage] = {}
        self.state = State.IDLE
        self.current_stage: Optional[str] = None
        self.logger: logging.Logger = logging.getLogger(__name__)

    def step(
        self, *, stage: Optional[str] = None, depends_on: Optional[List[str]] = None
    ):
        """Decorator to add a step to the pipeline"""

        def decorator(func: Callable):
            step_info = {
                "name": func.__name__,
                "func": func,
                "dependencies": depends_on or [],
                "signature": signature(func),
                "stage": stage,
            }
            self.steps.append(step_info)

            # Add to stage if specified
            if stage:
                if stage not in self.stages:
                    self.stages[stage] = Stage(name=stage)
                if self.stages[stage].steps is None:
                    self.stages[stage].steps = []
                self.stages[stage].steps.append(func.__name__)

            return func

        return decorator

    def run(self, **initial_inputs):
        """Run the pipeline with given inputs"""
        self.state = State.RUNNING
        results = initial_inputs

        try:
            for step in self.steps:
                # Update stage state
                if step["stage"]:
                    self.current_stage = step["stage"]
                    self.stages[step["stage"]].state = State.RUNNING

                # Wait for dependencies
                if step["dependencies"]:
                    for dep in step["dependencies"]:
                        if dep not in results:
                            raise ValueError(f"Dependency {dep} not satisfied")

                # Match function parameters with available results
                params = {}
                for param_name in step["signature"].parameters:
                    if param_name in results:
                        params[param_name] = results[param_name]

                # Run the step
                result = step["func"](**params)

                # Store result
                results[step["name"]] = result

                # Update stage state
                if step["stage"]:
                    self.stages[step["stage"]].state = State.COMPLETED

            self.state = State.COMPLETED
            return results

        except Exception as e:
            self.state = State.ERROR
            if self.current_stage:
                self.stages[self.current_stage].state = State.ERROR
            raise

    def save(self, storage: StorageBackend):
        """Save pipeline state to storage"""
        storage.save_pipeline(self)

    @classmethod
    def load(cls, name: str, storage: StorageBackend):
        """Load pipeline from storage"""
        return storage.load_pipeline(name)
