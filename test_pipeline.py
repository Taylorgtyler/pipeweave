from pipeweave.core import Pipeline
from pipeweave.step import State
from pipeweave.stage import Stage
from pipeweave.step import Step


def test_basic_pipeline():
    # Create pipeline
    pipeline = Pipeline(name="test_pipeline")

    # Test function
    def double_number(x):
        return x * 2

    # Add step
    pipeline.add_step(
        name="double",
        description="Double the input",
        function=double_number,
        inputs=["number"],
        outputs=["result"],
    )

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

    pipeline.add_step(
        name="add_one",
        description="Add one",
        function=add_one,
        inputs=["number"],
        outputs=["result"],
    )

    pipeline.add_step(
        name="multiply",
        description="Multiply by two",
        function=multiply_by_two,
        inputs=["result"],
        outputs=["final"],
        dependencies={"add_one"},
    )

    result = pipeline.run(5)

    assert pipeline.state == State.COMPLETED
    assert result["multiply"]["final"] == 12


def test_pipeline_error_handling():
    pipeline = Pipeline(name="test_error")

    def raise_error(x):
        raise ValueError("Test error")

    pipeline.add_step(
        name="error_step",
        description="Raise error",
        function=raise_error,
        inputs=["number"],
        outputs=["result"],
    )

    try:
        pipeline.run(5)
        assert False, "Should have raised an error"
    except ValueError:
        assert pipeline.state == State.ERROR
        assert pipeline.steps["error_step"].state == State.ERROR


def create_step(name, description, function, inputs, outputs):
    return Step(
        name=name,
        description=description,
        function=function,
        inputs=inputs,
        outputs=outputs,
    )


def create_stage(name, description, steps):
    return Stage(
        name=name,
        description=description,
        steps=steps,
    )


def test_pipeline_with_stages():
    # Create pipeline
    pipeline = Pipeline(name="test_pipeline_with_stages")

    # Define step functions
    def double_number(x):
        return x * 2

    def add_one(x):
        return x + 1

    # Create steps using helper function
    step_double = create_step(
        "double", "Double the input", double_number, ["number"], ["result"]
    )
    step_add_one = create_step(
        "add_one", "Add one to the input", add_one, ["result"], ["final"]
    )

    # Create a stage using helper function
    stage_processing = create_stage(
        "processing_stage", "Stage for processing data", [step_double, step_add_one]
    )

    # Add the stage to the pipeline with the correct arguments
    pipeline.add_stage(
        name=stage_processing.name,
        description=stage_processing.description,
        steps=stage_processing.steps,
    )

    # Run the pipeline
    result = pipeline.run(5)

    # Assertions
    assert pipeline.state == State.COMPLETED
    assert "double" in result
    assert result["double"]["result"] == 10
    assert "add_one" in result
    assert result["add_one"]["final"] == 11
