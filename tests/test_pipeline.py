from pipeweave.core import Pipeline
from pipeweave.step import State


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
