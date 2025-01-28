# Pipeweave

A flexible Python data pipeline library that makes it easy to construct and run custom data pipelines using a finite state machine approach.

## Project Goal

I have tried some popular Python data pipeline libraries, and have found them all to be a little hard to use for custom use cases. The goal of this project is to create a pipeline library that avoids some of the common pitfalls and allows users to easily construct pipelines using custom functions and run them using a finite state machine.

## Features

- 🚀 Simple, intuitive API for creating data pipelines
- 🔄 Built-in state management using finite state machines
- 📦 Easy integration of custom functions
- 🎯 Flexible decorator API with input mapping
- 💾 Multiple storage backends (SQLite included)
- 🔍 Pipeline status tracking and monitoring
- ⚡ Efficient execution with dependency management

## Installation

```bash
pip install pipeweave
```

## Quick Start

Here's a simple example that demonstrates how to create and run a pipeline using the decorator API:

```python
from pipeweave import Pipeline

# Create a pipeline
pipeline = Pipeline(name="text_processor")

# Define steps using decorators
@pipeline.step(stage="cleaning")
def clean_text(text: str) -> str:
    """Clean text by converting to lowercase and stripping whitespace."""
    return text.strip().lower()

@pipeline.step(stage="cleaning")
def count_words(cleaned: str) -> int:
    """Count words in cleaned text."""
    return len(cleaned.split())

# Run the pipeline with input text
text = "  Hello World  "
results = pipeline.run(text=text)
# Data flows: "  Hello World  " -> "hello world" -> 2

print(results["clean_text"]["result"])  # "hello world"
print(results["count_words"]["result"])  # 2
```

You can also create pipelines using the traditional API:

```python
from pipeweave import Pipeline, create_step, create_stage

# Create a pipeline
pipeline = Pipeline(name="text_processor")

# Create a stage with sequential steps
cleaning_stage = create_stage(
    name="cleaning",
    description="Clean and process text",
    steps=[
        create_step(
            name="clean_text",
            description="Clean the text",
            function=clean_text,
            inputs=["text"],
            outputs=["cleaned"],
        ),
        create_step(
            name="count_words",
            description="Count words in text",
            function=count_words,
            inputs=["cleaned"],
            outputs=["word_count"],
        ),
    ],
)

# Add stage to pipeline
pipeline.add_stage(cleaning_stage)

# Run the pipeline with input text
results = pipeline.run(text="  Hello World  ")
```

## Core Concepts

### Steps

A Step is the basic building block of a pipeline. Each step:
- Has a unique name and description
- Contains a processing function
- Defines its inputs and outputs
- Receives input from the previous step's output by default
- Can specify dependencies for custom data flow
- Maintains its own state (IDLE, RUNNING, COMPLETED, ERROR)

### Stages

A Stage is a collection of steps that are executed sequentially. Each stage:
- Has a unique name and description
- Contains multiple steps that form a data transformation flow
- Passes data between steps automatically
- Can specify dependencies on other stages
- Maintains its own state (IDLE, RUNNING, COMPLETED, ERROR)

Stages provide a natural way to organize related data transformations, where each step builds on the output of the previous step.

### Pipeline

A Pipeline manages the flow of data through steps and stages:
- Executes steps and stages in dependency order
- Passes data between components automatically
- Tracks execution state
- Can be saved and loaded using storage backends
- Supports both decorator and traditional APIs

### Storage Backends

Pipeweave supports different storage backends for persisting pipelines:
- SQLite (included)
- Custom backends can be implemented using the StorageBackend base class

## Advanced Usage

### Using the Decorator API

The decorator API provides a clean, intuitive way to create pipelines:

```python
from pipeweave import Pipeline

pipeline = Pipeline(name="number_processor")

# Define steps with input mapping
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

# Run pipeline with named input
results = pipeline.run(number=5)
# Data flows:
# - math_ops stage: 5 -> double_number (10) -> add_one (11)
# - format_number: 5 -> "Original number: 5"

print(results["double_number"]["result"])  # 10
print(results["add_one"]["result"])  # 11
print(results["format_number"]["result"])  # "Original number: 5"
```

### Using Dependencies

You can create complex data flows using explicit dependencies:

```python
from pipeweave import Pipeline

pipeline = Pipeline(name="dependency_example")

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
results = pipeline.run()
# Data flows: generate_number (5) -> double_number (10) -> add_one (11)
```

### Using Storage Backends
```python
from pipeweave import Pipeline, create_step
from pipeweave.storage import SQLiteStorage

# Create a pipeline
pipeline = Pipeline(name="data_transformer")

# Add steps using decorator
@pipeline.step()
def transform(x: int) -> int:
    return x * 2

# Initialize Storage
storage = SQLiteStorage("pipelines.db")

# Save Pipeline
storage.save_pipeline(pipeline)

# Load Pipeline
loaded_pipeline = storage.load_pipeline("data_transformer")
```

### Error Handling
```python
from pipeweave import Pipeline, State

pipeline = Pipeline(name="error_example")

@pipeline.step()
def will_fail(x: int) -> int:
    raise ValueError("Example error")

try:
    results = pipeline.run(x=5)
except Exception as e:
    # Check state of steps
    for step in pipeline.steps.values():
        if step.state == State.ERROR:
            print(f"Step {step.name} failed")
```

## Contributing

Contributions are welcome! This is a new project, so please feel free to open issues and suggest improvements.

For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Project Status

This project is actively maintained and under development. Current version: 0.4.0

## Roadmap

- [x] Add a stages feature
- [ ] Add a more robust state machine implementation
- [ ] Add postgres storage backend
- [ ] Add more detailed monitoring and logging
- [x] Add more testing and CI/CD pipeline
- [ ] Add a cli
- [ ] Add more metadata to pipelines, stages, and steps
- [ ] Add a web app management interface