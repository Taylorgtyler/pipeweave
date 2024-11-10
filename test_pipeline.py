from pipeweave.core import Pipeline, create_step, create_stage
from pipeweave.step import State


def test_basic_pipeline():
    # Create pipeline
    pipeline = Pipeline(name="test_pipeline")

    # Test function
    def double_number(x):
        return x * 2

    # Create and add step
    step = create_step(
        name="double",
        description="Double the input",
        function=double_number,
        inputs=["number"],
        outputs=["result"],
    )
    pipeline.add_step(step)

    # Run pipeline
    result = pipeline.run(5)

    assert pipeline.state == State.COMPLETED
    assert "double" in result
    assert result["double"]["result"] == 10


def test_pipeline_dependencies():
    pipeline = Pipeline(name="test_dependencies")

    def add_one(x):
        return x + 1

    def multiply_by_two(x):
        return x * 2

    # Create steps using create_step helper
    step_add = create_step(
        name="add_one",
        description="Add one",
        function=add_one,
        inputs=["number"],
        outputs=["result"],
    )

    step_multiply = create_step(
        name="multiply",
        description="Multiply by two",
        function=multiply_by_two,
        inputs=["result"],
        outputs=["final"],
        dependencies={"add_one"},
    )

    # Add steps to pipeline
    pipeline.add_step(step_add)
    pipeline.add_step(step_multiply)

    result = pipeline.run(5)

    assert pipeline.state == State.COMPLETED
    assert result["multiply"]["final"] == 12


def test_pipeline_error_handling():
    pipeline = Pipeline(name="test_error")

    def raise_error(x):
        raise ValueError("Test error")

    # Create step using create_step helper
    error_step = create_step(
        name="error_step",
        description="Raise error",
        function=raise_error,
        inputs=["number"],
        outputs=["result"],
    )

    pipeline.add_step(error_step)

    try:
        pipeline.run(5)
        assert False, "Should have raised an error"
    except ValueError:
        assert pipeline.state == State.ERROR
        assert pipeline.steps["error_step"].state == State.ERROR


def test_pipeline_with_stages():
    # Create pipeline
    pipeline = Pipeline(name="test_pipeline_with_stages")

    # Define step functions
    def double_number(x):
        return x * 2

    def add_one(x):
        return x + 1

    # Create steps
    step_double = create_step(
        "double", "Double the input", double_number, ["number"], ["result"]
    )
    step_add_one = create_step(
        "add_one", "Add one to the input", add_one, ["result"], ["final"]
    )

    # Create a stage
    stage = create_stage(
        "processing_stage", 
        "Stage for processing data", 
        [step_double, step_add_one]
    )

    # Add the stage to the pipeline
    pipeline.add_stage(stage)

    # Run the pipeline
    result = pipeline.run(5)

    assert pipeline.state == State.COMPLETED
    assert "double" in result
    assert result["double"]["result"] == 10
    assert "add_one" in result
    assert result["add_one"]["final"] == 11
