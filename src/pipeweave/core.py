from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Set
import logging
from .step import Step, State

class Pipeline:
    """A generic pipeline class that manages the execution of data processing steps."""
    
    def __init__(self, name: str):
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
        dependencies: Optional[Set[str]] = None
    ) -> Pipeline:
        """Add a processing step to the pipeline."""
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
            dependencies=dependencies
        )
        return self

    def _get_execution_order(self) -> List[Step]:
        """Determine the correct execution order based on dependencies."""
        executed = set()
        execution_order = []
        
        def can_execute(step: Step) -> bool:
            return all(dep in executed for dep in step.dependencies)
            
        while len(executed) < len(self.steps):
            ready_steps = [
                step for name, step in self.steps.items()
                if name not in executed and can_execute(step)
            ]
            
            if not ready_steps and len(executed) < len(self.steps):
                raise ValueError("Circular dependency detected in pipeline")
                
            execution_order.extend(ready_steps)
            executed.update(step.name for step in ready_steps)
            
        return execution_order

    def run(self, input_data: Any = None) -> Dict[str, Any]:
        """Execute the pipeline with the given input data."""
        self.state = State.RUNNING
        current_data = input_data
        
        try:
            ordered_steps = self._get_execution_order()
            
            for step in ordered_steps:
                self.current_step = step
                self.logger.info(f"Executing step: {step.name}")
                
                # Prepare input data based on dependencies and inputs
                if step.dependencies:
                    dep_data = {
                        output: self.results[dep][output]
                        for dep in step.dependencies
                        for output in self.steps[dep].outputs
                        if output in step.inputs
                    }
                    if current_data is not None:
                        dep_data.update(current_data)
                    current_data = step.execute(dep_data)
                else:
                    current_data = step.execute(current_data)
                
                # Store results with output names
                if isinstance(current_data, dict):
                    self.results[step.name] = current_data
                else:
                    # If single output, use first output name
                    self.results[step.name] = {step.outputs[0]: current_data}
                
            self.state = State.COMPLETED
            return self.results
            
        except Exception as e:
            self.state = State.ERROR
            if self.current_step:
                self.current_step.state = State.ERROR
            self.logger.error(f"Pipeline error in step {self.current_step.name}: {str(e)}")
            raise

    def reset(self) -> None:
        """Reset the pipeline to its initial state."""
        self.state = State.IDLE
        self.current_step = None
        self.results.clear()
        for step in self.steps.values():
            step.reset()