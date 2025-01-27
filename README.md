# Pipeweave

A flexible Python data pipeline library that makes it easy to construct and run custom data pipelines using a finite state machine approach.

## Project Goal

I have tried some popular Python data pipeline libraries, and have found them all to be a little hard to use for custom use cases. The goal of this project is to create a pipeline library that avoids some of the common pitfalls and allows users to easily construct pipelines using custom functions and run them using a finite state machine.

## Features

- 🚀 Simple, intuitive API for creating data pipelines
- 🔄 Built-in state management using finite state machines
- 📦 Easy integration of custom functions
- 💾 Multiple storage backends (SQLite included)
- 🔍 Pipeline status tracking and monitoring
- ⚡ Efficient execution with dependency management

## Installation

```bash
pip install pipeweave
```

## Quick Start

Here's a simple example that demonstrates how to create and run a pipeline:

```python
from pipeweave import Pipeline, create_step, create_stage

# Create a pipeline
pipeline = Pipeline(name="text_processor")

# Define processing functions
def clean_text(text):
    """Clean text by converting to lowercase and stripping whitespace."""
    return text.strip().lower()

def count_words(text):
    """Count words in cleaned text."""
    return len(text.split())

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
text = "  Hello World  "
results = pipeline.run(text)
# Data flows: "  Hello World  " -> "hello world" -> 2

print(results["clean_text"]["cleaned"])  # "hello world"
print(results["count_words"]["word_count"])  # 2
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

### Storage Backends

Pipeweave supports different storage backends for persisting pipelines:
- SQLite (included)
- Custom backends can be implemented using the StorageBackend base class

## Advanced Usage

### Using Storage Backends
```python
from pipeweave import Pipeline, create_step
from pipeweave.storage import SQLiteStorage

# Create a pipeline
pipeline = Pipeline(name="data_transformer")

# Add steps
step = create_step(
    name="example_step",
    description="Example step",
    function=lambda x: x * 2,
    inputs=["input"],
    outputs=["output"],
)
pipeline.add_step(step)

# Initialize Storage
storage = SQLiteStorage("pipelines.db")

# Save Pipeline
storage.save_pipeline(pipeline)

# Load Pipeline
loaded_pipeline = storage.load_pipeline("data_transformer")
```

### Error Handling
```python
from pipeweave import Pipeline, create_step, State

# Create pipeline with a step that will fail
def will_fail(x):
    raise ValueError("Example error")

error_step = create_step(
    name="error_step",
    description="This step will fail",
    function=will_fail,
    inputs=["data"],
    outputs=["result"],
)

pipeline = Pipeline(name="error_example")
pipeline.add_step(error_step)

try:
    results = pipeline.run(data)
except Exception as e:
    # Check state of steps
    for step in pipeline.steps.values():
        if step.state == State.ERROR:
            print(f"Step {step.name} failed: {step.error}")
```

### Advanced Data Flow Patterns

Here's an example showing different data flow patterns:

```python
from pipeweave import Pipeline, create_step, create_stage

# Create a pipeline for processing numbers
pipeline = Pipeline(name="number_processor")

# Stage 1: Sequential number processing
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

# Independent step that works with original input
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

# Run pipeline with input 5
results = pipeline.run(5)
# Data flows:
# - math_stage: 5 -> double (10) -> add_one (11)
# - format_step: 5 -> "Original number: 5"

print(results["double"]["doubled"])  # 10
print(results["add_one"]["result"])  # 11
print(results["format"]["formatted"])  # "Original number: 5"
```

For independent operations that need to work with the original input data, you can:
1. Use stages with single steps
2. Use explicit dependencies (when supported)
3. Create separate pipelines

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

Pipeweave is currently in alpha. While it's functional and tested, the API may change as we gather user feedback and add new features.

## Roadmap

- [x] Add a stages feature
- [ ] Add a more robust state machine implementation
- [ ] Add postgres storage backend
- [ ] Add more detailed monitoring and logging
- [x] Add more testing and CI/CD pipeline
- [ ] Add a cli
- [ ] Add more metadata to pipelines, stages, and steps
- [ ] Add a web app management interface