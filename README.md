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

```bash
pip install pipeweave

```

## Quick Start
Here's a simple example that demonstrates how to create and run a pipeline:

```python
from pipeweave.core import Pipeline, create_step, create_stage

# Create a pipeline
pipeline = Pipeline(name="data_transformer")

# Define processing functions
def clean_data(data):
    return [x.strip().lower() for x in data]

def filter_empty(data):
    return [x for x in data if x]

# Create steps
clean_step = create_step(
    name="clean_data",
    description="Clean the data",
    function=clean_data,
    inputs=["raw_data"],
    outputs=["cleaned_data"],
)

filter_step = create_step(
    name="filter_empty",
    description="Filter out empty strings",
    function=filter_empty,
    inputs=["cleaned_data"],
    outputs=["filtered_data"],
    dependencies={"clean_data"},
)

# Add steps to the pipeline
pipeline.add_step(clean_step)
pipeline.add_step(filter_step)

# Run the pipeline
data = [" Hello ", "World ", "", " Python "]
results = pipeline.run(data)

print(results)
```


## Core Concepts

### Steps

A Step is the basic building block of a pipeline. Each step:
- Has a unique name
- Contains a processing function
- Defines its inputs and outputs
- Can specify dependencies on other steps
- Maintains its own state (IDLE, RUNNING, COMPLETED, ERROR)


### Stages

A Stage is a collection of steps that can be executed together. Each stage:
- Has a unique name and description
- Contains multiple steps, which are individual processing units
- Defines its own state (IDLE, RUNNING, COMPLETED, ERROR)
- Can specify dependencies on other stages, ensuring that it only runs when all its dependencies have been completed

Stages allow for better organization of complex pipelines by grouping related steps together. This modular approach enhances readability and maintainability of the pipeline code.


### Pipeline

A Pipeline is a collection of steps that:
- Manages the execution order based on dependencies
- Handles data flow between steps
- Tracks overall execution state
- Can be saved and loaded using storage backends

### Storage Backends

Pipeweave supports different storage backends for persisting pipelines:
- SQLite (included)
- Custom backends can be implemented using the StorageBackend base class

## Advanced Usage

### Using Storage Backends
```python
from pipeweave.core import Pipeline, create_step
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
from pipeweave.core import Pipeline, create_step
from pipeweave.step import State

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
### Stages
```python
from pipeweave.core import Pipeline, create_step, create_stage

# Create a pipeline
pipeline = Pipeline(name="data_transformer")

# Define step functions
def double_number(x):
    return x * 2

def add_one(x):
    return x + 1

# Create steps
step_double = create_step(
    name="double",
    description="Double the input",
    function=double_number,
    inputs=["number"],
    outputs=["result"],
)

step_add_one = create_step(
    name="add_one",
    description="Add one to the input",
    function=add_one,
    inputs=["result"],
    outputs=["final"],
)

# Create a stage
processing_stage = create_stage(
    name="processing_stage",
    description="Process the data",
    steps=[step_double, step_add_one],
)

# Add stage to pipeline
pipeline.add_stage(processing_stage)

# Run the pipeline
results = pipeline.run(5)
print(results)
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

Pipeweave is currently in alpha. While it's functional and tested, the API may change as we gather user feedback and add new features.

## Roadmap

- [x] Add a stages feature
- [ ] Add a more robust state machine implementation
- [ ] Add more storage backends
- [ ] Add more detailed monitoring and logging
- [ ] Add more testing and CI/CD pipeline