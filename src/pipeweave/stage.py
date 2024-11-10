from typing import List, Set, Any
from dataclasses import dataclass, field

from .step import Step, State


@dataclass
class Stage:
    """A collection of steps that can be executed together."""

    name: str
    description: str
    steps: List[Step]
    state: State = State.IDLE
    dependencies: Set[str] = field(default_factory=set)

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

    def execute(self, data: Any) -> dict:
        """Execute all steps in the stage with the provided input data."""
        try:
            self.state = State.RUNNING
            results = {}

            # Use the initial data for the first step
            current_data = data

            for step in self.steps:
                # Execute the step with the current data
                step_result = step.execute(current_data)

                # Store results with step name as key
                if len(step.outputs) > 1:
                    results.update(step_result)
                else:
                    results[step.name] = {
                        step.outputs[0]: step_result
                    }  # Use step name as key

                # Update current_data for the next step
                if step.outputs:
                    # If the step returns a dictionary, use the output name to get the value
                    if isinstance(step_result, dict):
                        current_data = step_result[step.name][
                            step.outputs[0]
                        ]  # Use results dict
                    else:
                        current_data = (
                            step_result  # If it's a single value, use it directly
                        )

            self.state = State.COMPLETED
            return results
        except Exception as e:
            self.state = State.ERROR
            raise

    def reset(self) -> None:
        """Reset the stage and all its steps to their initial state."""
        self.state = State.IDLE
        for step in self.steps:
            step.reset()
