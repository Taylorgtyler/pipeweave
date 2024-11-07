from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Set
import logging
from .step import Step, State
from .storage.base import StorageBackend


class Pipeline:
    """A generic pipeline class that manages the execution of data processing steps.

    The Pipeline class provides functionality to create and execute data processing pipelines
    by managing a series of interconnected steps. It handles dependency resolution,
    execution order, and maintains state throughout the pipeline's lifecycle.

    Attributes:
        name (str): The name of the pipeline
        steps (Dict[str, Step]): Dictionary mapping step names to Step objects
        state (State): Current state of the pipeline (IDLE, RUNNING, COMPLETED, ERROR)
        current_step (Optional[Step]): Reference to the currently executing step
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
        self.state: State = State.IDLE
        self.current_step: Optional[Step] = None
        self.results: Dict[str, Any] = {}
        self.logger = logging.getLogger(__name__)

    def add_step(
        self,
        name: str,
        description: str,
        function: Callable[[Any], Any],
        inputs: List[str],
        outputs: List[str],
        dependencies: Optional[Set[str]] = None,
    ) -> Pipeline:
        """Add a processing step to the pipeline.

        Args:
            name (str): Unique identifier for the step
            description (str): Human-readable description of the step's purpose
            function (Callable[[Any], Any]): The function to execute for this step
            inputs (List[str]): List of input names expected by the function
            outputs (List[str]): List of output names produced by the function
            dependencies (Optional[Set[str]], optional): Set of step names that must execute before this step. Defaults to None.

        Returns:
            Pipeline: The pipeline instance for method chaining

        Raises:
            ValueError: If a dependency is specified that doesn't exist in the pipeline
        """
        if dependencies is None:
            dependencies = set()

        # Validate dependencies exist
        for dep in dependencies:
            if dep not in self.steps:
                raise ValueError(f"Dependency '{dep}' not found in pipeline steps")

        self.steps[name] = Step(
            name=name,
            description=description,
            function=function,
            inputs=inputs,
            outputs=outputs,
            dependencies=dependencies,
        )
        return self

    def _get_execution_order(self) -> List[Step]:
        """Determine the correct execution order based on dependencies.

        This method performs a topological sort of the steps based on their dependencies
        to determine a valid execution order.

        Returns:
            List[Step]: List of steps in the order they should be executed

        Raises:
            ValueError: If a circular dependency is detected in the pipeline
        """
        executed = set()
        execution_order = []

        def can_execute(step: Step) -> bool:
            return all(dep in executed for dep in step.dependencies)

        while len(executed) < len(self.steps):
            ready_steps = [
                step
                for name, step in self.steps.items()
                if name not in executed and can_execute(step)
            ]

            if not ready_steps and len(executed) < len(self.steps):
                raise ValueError("Circular dependency detected in pipeline")

            execution_order.extend(ready_steps)
            executed.update(step.name for step in ready_steps)

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
            ordered_steps = self._get_execution_order()

            for step in ordered_steps:
                self.current_step = step
                self.logger.info(f"Executing step: {step.name}")

                # Prepare input data based on dependencies and inputs
                dep_data = {}

                if step.dependencies:
                    dep_data = {
                        output: self.results[dep][output]
                        for dep in step.dependencies
                        for output in self.steps[dep].outputs
                        if output in step.inputs
                    }
                    if len(step.inputs) == 1 and step.inputs[0] in dep_data:
                        dep_data = {step.inputs[0]: dep_data[step.inputs[0]]}

                if current_data is not None:
                    input_dict = {step.inputs[0]: current_data} if isinstance(current_data, (int, float, str)) else current_data
                    dep_data.update(input_dict)

                current_data = step.execute(dep_data)

                # Store results with output names
                if isinstance(current_data, dict):
                    self.results[step.name] = current_data
                else:
                    self.results[step.name] = {step.outputs[0]: current_data}

            self.state = State.COMPLETED
            return self.results

        except Exception as e:
            self.state = State.ERROR
            if self.current_step:
                self.current_step.state = State.ERROR
            self.logger.error(
                f"Pipeline error in step {self.current_step.name}: {str(e)}"
            )
            raise


    def reset(self) -> None:
        """Reset the pipeline to its initial state.

        This method clears all results and resets the state of the pipeline and all its steps
        back to IDLE, allowing the pipeline to be run again.
        """
        self.state = State.IDLE
        self.current_step = None
        self.results.clear()
        for step in self.steps.values():
            step.reset()
    
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
