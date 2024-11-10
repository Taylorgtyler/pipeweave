from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Set, Union
import logging
from .step import Step, State
from .stage import Stage
from .storage.base import StorageBackend

def create_step(
    name: str,
    description: str,
    function: Callable[[Any], Any],
    inputs: List[str],
    outputs: List[str],
    dependencies: Optional[Set[str]] = None,
) -> Step:
    """Create a new Step instance.

    Args:
        name (str): Unique identifier for the step
        description (str): Human-readable description of the step's purpose
        function (Callable[[Any], Any]): The function to execute for this step
        inputs (List[str]): List of input names expected by the function
        outputs (List[str]): List of output names produced by the function
        dependencies (Optional[Set[str]], optional): Set of step names that must execute before this step

    Returns:
        Step: A new Step instance
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

    Args:
        name (str): Unique identifier for the stage
        description (str): Human-readable description of the stage's purpose
        steps (List[Step]): List of steps in this stage
        dependencies (Optional[Set[str]], optional): Set of stage names that must execute before this stage

    Returns:
        Stage: A new Stage instance
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
    by managing a series of interconnected steps. It handles dependency resolution,
    execution order, and maintains state throughout the pipeline's lifecycle.

    Attributes:
        name (str): The name of the pipeline
        steps (Dict[str, Step]): Dictionary mapping step names to Step objects
        stages (Dict[str, Stage]): Dictionary mapping stage names to Stage objects
        state (State): Current state of the pipeline (IDLE, RUNNING, COMPLETED, ERROR)
        current_step (Optional[Step]): Reference to the currently executing step
        current_stage (Optional[Stage]): Reference to the currently executing stage
        results (Dict[str, Any]): Dictionary storing the results of each step
        logger (Logger): Logger instance for pipeline execution logs
    """

    def __init__(self, name: str):
        """Initialize a new Pipeline instance.

        Args:
            name (str): The name of the pipeline
        """
        self.name = name
        self.steps: Dict[str, Step] = {}
        self.stages: Dict[str, Stage] = {}
        self.state: State = State.IDLE
        self.current_step: Optional[Step] = None
        self.current_stage: Optional[Stage] = None
        self.results: Dict[str, Any] = {}
        self.logger = logging.getLogger(__name__)

    def add_step(self, step: Step) -> Pipeline:
        """Add a step to the pipeline.

        Args:
            step (Step): The step to add to the pipeline

        Returns:
            Pipeline: The pipeline instance for method chaining

        Raises:
            ValueError: If a dependency is specified that doesn't exist in the pipeline
        """
        # Validate dependencies exist
        for dep in step.dependencies:
            if dep not in self.steps:
                raise ValueError(f"Dependency '{dep}' not found in pipeline steps")

        self.steps[step.name] = step
        return self

    def add_stage(self, stage: Stage) -> Pipeline:
        """Add a stage to the pipeline.

        Args:
            stage (Stage): The stage to add to the pipeline

        Returns:
            Pipeline: The pipeline instance for method chaining

        Raises:
            ValueError: If a stage dependency is specified that doesn't exist in the pipeline
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
        """Determine the correct execution order based on dependencies."""
        executed = set()
        execution_order = []

        def can_execute(item: Union[Step, Stage]) -> bool:
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

    def run(self, input_data: Any = None) -> Dict[str, Any]:
        """Execute the pipeline with the given input data.

        This method executes all steps in the pipeline in dependency order, passing data
        between steps and handling any errors that occur during execution.

        Args:
            input_data (Any, optional): Initial input data to pass to the pipeline. Defaults to None.

        Returns:
            Dict[str, Any]: Dictionary containing the results of all pipeline steps, with step names as keys
                           and their outputs as values.

        Raises:
            ValueError: If a circular dependency is detected in the pipeline
            Exception: If any step fails during execution, the original exception is re-raised
        """
        self.state = State.RUNNING
        current_data = input_data

        try:
            ordered_items = self._get_execution_order()

            for item in ordered_items:
                if isinstance(item, Stage):
                    self.current_stage = item
                    self.logger.info(f"Executing stage: {item.name}")
                    stage_results = item.execute(current_data)
                    self.results.update(stage_results)
                    # Pass the output of the stage to the next stage or step
                    current_data = (
                        stage_results  # Update current_data to the results of the stage
                    )
                else:  # Step
                    self.current_step = item
                    self.logger.info(f"Executing step: {item.name}")

                    # Prepare input data based on dependencies and inputs
                    dep_data = {}

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

        This method clears all results and resets the state of the pipeline and all its steps
        back to IDLE, allowing the pipeline to be run again.
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

        Args:
            storage (StorageBackend): Storage backend to save to
        """
        storage.save_pipeline(self)

    @classmethod
    def load(cls, storage: StorageBackend, pipeline_name: str) -> "Pipeline":
        """Load pipeline from storage backend.

        Args:
            storage (StorageBackend): Storage backend to load from
            pipeline_name (str): Name of pipeline to load

        Returns:
            Pipeline: Loaded pipeline instance
        """
        return storage.load_pipeline(pipeline_name)
