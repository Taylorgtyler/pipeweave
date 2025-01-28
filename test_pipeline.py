"""Tests for the Pipeline class and its data flow patterns."""

from pipeweave import Pipeline, create_step, create_stage, State


def test_basic_pipeline():
    """Test basic pipeline with a single step."""
    pipeline = Pipeline(name="test_pipeline", description="Test basic data flow")

    # Create and add step
    step = create_step(
        name="double",
        description="Double the input",
        function=lambda x: x * 2,
        inputs=["number"],
        outputs=["result"],
    )
    pipeline.add_step(step)

    # Run pipeline with raw input
    result = pipeline.run(5)  # Input: 5 -> double (10)

    assert pipeline.state == State.COMPLETED
    assert "double" in result
    assert result["double"]["result"] == 10


def test_sequential_data_flow():
    """Test sequential data flow through multiple steps."""
    pipeline = Pipeline(name="test_flow", description="Test sequential data flow")

    # Create steps that will process data in sequence
    steps = [
        create_step(
            name="double",
            description="Double the input",
            function=lambda x: x * 2,
            inputs=["number"],
            outputs=["doubled"],
        ),
        create_step(
            name="add_one",
            description="Add one to doubled number",
            function=lambda x: x + 1,
            inputs=["doubled"],
            outputs=["result"],
        ),
    ]

    # Create stage for sequential processing
    stage = create_stage(
        name="math_ops",
        description="Mathematical operations",
        steps=steps,
    )

    # Add stage to pipeline
    pipeline.add_stage(stage)

    # Run pipeline with raw input
    result = pipeline.run(5)  # Input flows: 5 -> double (10) -> add_one (11)

    assert pipeline.state == State.COMPLETED
    assert result["double"]["doubled"] == 10
    assert result["add_one"]["result"] == 11


def test_independent_steps():
    """Test pipeline with both sequential and independent steps."""
    pipeline = Pipeline(
        name="test_independent", description="Test independent step patterns"
    )

    # Create sequential processing stage
    math_stage = create_stage(
        name="math_ops",
        description="Mathematical operations",
        steps=[
            create_step(
                name="double",
                description="Double the input",
                function=lambda x: x * 2,
                inputs=["number"],
                outputs=["doubled"],
            ),
            create_step(
                name="add_one",
                description="Add one to doubled number",
                function=lambda x: x + 1,
                inputs=["doubled"],
                outputs=["result"],
            ),
        ],
    )

    # Create independent step that works with original input
    format_step = create_step(
        name="format",
        description="Format the original number",
        function=lambda x: f"Original number: {x}",
        inputs=["number"],
        outputs=["formatted"],
    )

    # Add components to pipeline
    pipeline.add_stage(math_stage)
    pipeline.add_step(format_step)

    # Run pipeline with raw input
    result = pipeline.run(5)
    # Data flows:
    # - math_stage: 5 -> double (10) -> add_one (11)
    # - format_step: 5 -> "Original number: 5"

    assert pipeline.state == State.COMPLETED
    assert result["double"]["doubled"] == 10
    assert result["add_one"]["result"] == 11
    assert result["format"]["formatted"] == "Original number: 5"


def test_pipeline_error_handling():
    """Test error handling in pipeline execution."""
    pipeline = Pipeline(name="test_error", description="Test error handling")

    def raise_error(x):
        raise ValueError("Test error")

    # Create step that will fail
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


def test_decorator_pipeline():
    """Test pipeline creation using decorators."""
    pipeline = Pipeline(name="decorator_test", description="Test decorator API")

    # Define steps using decorators
    @pipeline.step(stage="math_ops", input_map={"x": "number"})
    def double_number(x: int) -> int:
        """Double the input number."""
        return x * 2

    @pipeline.step(stage="math_ops", input_map={"doubled": "result"})
    def add_one(doubled: int) -> int:
        """Add one to the doubled number."""
        return doubled + 1

    @pipeline.step(input_map={"x": "number"})  # Independent step
    def format_number(x: int) -> str:
        """Format the original number."""
        return f"Original number: {x}"

    # Run pipeline with raw input
    result = pipeline.run(number=5)
    # Data flows:
    # - math_ops stage: 5 -> double_number (10) -> add_one (11)
    # - format_number: 5 -> "Original number: 5"

    assert pipeline.state == State.COMPLETED
    assert result["double_number"]["result"] == 10
    assert result["add_one"]["result"] == 11
    assert result["format_number"]["result"] == "Original number: 5"


def test_decorator_dependencies():
    """Test pipeline with explicit dependencies using decorators."""
    pipeline = Pipeline(
        name="dependency_test", description="Test decorator dependencies"
    )

    @pipeline.step()
    def generate_number() -> int:
        """Generate a number."""
        return 5

    @pipeline.step(depends_on=["generate_number"], input_map={"number": "result"})
    def double_number(number: int) -> int:
        """Double the generated number."""
        return number * 2

    @pipeline.step(depends_on=["double_number"], input_map={"doubled": "result"})
    def add_one(doubled: int) -> int:
        """Add one to the doubled number."""
        return doubled + 1

    # Run pipeline
    result = pipeline.run()
    # Data flows: generate_number (5) -> double_number (10) -> add_one (11)

    assert pipeline.state == State.COMPLETED
    assert result["generate_number"]["result"] == 5
    assert result["double_number"]["result"] == 10
    assert result["add_one"]["result"] == 11
