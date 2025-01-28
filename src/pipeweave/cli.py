import click
import yaml
import json
import importlib
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, Callable
from datetime import datetime
from .core import Pipeline, create_step, create_stage
from .storage import SQLiteStorage
from .step import State


def load_config(path: str) -> Dict[str, Any]:
    """Load pipeline configuration from YAML/JSON file.

    Args:
        path (str): Path to configuration file (YAML or JSON)

    Returns:
        Dict[str, Any]: Pipeline configuration dictionary

    Raises:
        ValueError: If file format is not supported
        FileNotFoundError: If file does not exist
    """
    path = Path(path)
    if path.suffix not in [".yaml", ".yml", ".json"]:
        raise ValueError("Configuration file must be YAML or JSON")

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path) as f:
        if path.suffix in [".yaml", ".yml"]:
            return yaml.safe_load(f)
        else:
            return json.load(f)


def load_data_file(path: str) -> Any:
    """Load input data from file.

    Args:
        path (str): Path to data file (YAML or JSON)

    Returns:
        Any: Loaded data

    Raises:
        ValueError: If file format is not supported
        FileNotFoundError: If file does not exist
    """
    path = Path(path)
    if path.suffix not in [".yaml", ".yml", ".json"]:
        raise ValueError("Input data file must be YAML or JSON")

    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    with open(path) as f:
        if path.suffix in [".yaml", ".yml"]:
            return yaml.safe_load(f)
        else:
            return json.load(f)


def save_results(results: Dict[str, Any], path: str) -> None:
    """Save pipeline results to file.

    Args:
        results (Dict[str, Any]): Pipeline execution results
        path (str): Output file path (YAML or JSON)

    Raises:
        ValueError: If file format is not supported
    """
    path = Path(path)
    with open(path, "w") as f:
        if path.suffix in [".yaml", ".yml"]:
            yaml.dump(results, f, sort_keys=False)
        elif path.suffix == ".json":
            json.dump(results, f, indent=2)
        else:
            raise ValueError("Output file must be YAML or JSON")


def import_function(function_path: str) -> Callable:
    """Import a function from a module path string.

    Args:
        function_path (str): Dot-separated path to function (e.g. 'module.submodule.function')

    Returns:
        Callable: Imported function

    Raises:
        ImportError: If function cannot be imported
    """
    try:
        module_path, function_name = function_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, function_name)
    except Exception as e:
        raise ImportError(f"Could not import function {function_path}: {str(e)}")


def create_pipeline_from_config(config: Dict[str, Any]) -> Pipeline:
    """Create a pipeline from configuration dictionary.

    Args:
        config (Dict[str, Any]): Pipeline configuration

    Returns:
        Pipeline: Configured pipeline instance

    Raises:
        ValueError: If configuration is invalid
        ImportError: If function imports fail
    """
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a dictionary")

    required_fields = {"name", "steps"}
    if not all(field in config for field in required_fields):
        raise ValueError(f"Configuration missing required fields: {required_fields}")

    pipeline = Pipeline(name=config["name"])

    # Process stages if present
    if "stages" in config:
        for stage_config in config["stages"]:
            stage_steps = []
            for step_config in stage_config["steps"]:
                function = import_function(step_config["function"])
                step = create_step(
                    name=step_config["name"],
                    description=step_config.get("description", ""),
                    function=function,
                    inputs=step_config["inputs"],
                    outputs=step_config["outputs"],
                    dependencies=set(step_config.get("dependencies", [])),
                )
                stage_steps.append(step)

            stage = create_stage(
                name=stage_config["name"],
                description=stage_config.get("description", ""),
                steps=stage_steps,
                dependencies=set(stage_config.get("dependencies", [])),
            )
            pipeline.add_stage(stage)

    # Process individual steps
    for step_config in config["steps"]:
        if not isinstance(step_config, dict):
            raise ValueError(f"Step configuration must be a dictionary: {step_config}")

        required_step_fields = {"name", "function", "inputs", "outputs"}
        if not all(field in step_config for field in required_step_fields):
            raise ValueError(
                f"Step configuration missing required fields: {required_step_fields}"
            )

        function = import_function(step_config["function"])
        step = create_step(
            name=step_config["name"],
            description=step_config.get("description", ""),
            function=function,
            inputs=step_config["inputs"],
            outputs=step_config["outputs"],
            dependencies=set(step_config.get("dependencies", [])),
        )
        pipeline.add_step(step)

    return pipeline


@click.group()
def cli():
    """Pipeweave CLI tools for managing data pipelines."""
    pass


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--db-path", "-d", type=click.Path(), help="Path to SQLite database")
@click.option(
    "--input-data",
    "-i",
    type=click.Path(exists=True),
    help="Path to input data file (JSON/YAML)",
)
@click.option("--save/--no-save", default=True, help="Save pipeline to database")
@click.option("--output", "-o", type=click.Path(), help="Path to save output results")
def run(
    config_path: str,
    db_path: Optional[str],
    input_data: Optional[str],
    save: bool,
    output: Optional[str],
) -> None:
    """Run a pipeline from a configuration file.

    CONFIG_PATH should be a YAML or JSON file containing the pipeline configuration.

    Example config.yaml:
    ```yaml
    name: data_transformer
    description: Transform input data
    steps:
      - name: step1
        description: Double the input
        function: my_module.functions.double_number
        inputs: [number]
        outputs: [result]

      - name: step2
        description: Add 10 to the result
        function: my_module.functions.add_ten
        inputs: [result]
        outputs: [final_result]
        dependencies: [step1]
    ```
    """
    try:
        # Load configuration
        config = load_config(config_path)

        # Create pipeline
        pipeline = create_pipeline_from_config(config)

        # Load input data if provided
        input_data_dict = None
        if input_data:
            input_data_dict = load_data_file(input_data)

        # Initialize storage if needed
        storage = None
        if save and db_path:
            storage = SQLiteStorage(db_path)

        # Run pipeline
        click.echo(f"Running pipeline: {pipeline.name}")
        start_time = datetime.now()

        results = pipeline.run(input_data_dict)

        duration = (datetime.now() - start_time).total_seconds()
        click.echo(f"Pipeline completed in {duration:.2f} seconds")

        # Save pipeline if requested
        if save and storage:
            pipeline.save(storage)
            click.echo(f"Pipeline saved to database: {db_path}")

        # Save results if output path provided
        if output:
            save_results(results, output)
            click.echo(f"Results saved to: {output}")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()


@cli.command()
@click.argument("output_path", type=click.Path())
def init_config(output_path: str) -> None:
    """Create a template pipeline configuration file.

    OUTPUT_PATH should end in .yaml, .yml, or .json
    """
    template = {
        "name": "my_pipeline",
        "description": "Pipeline description",
        "stages": [
            {
                "name": "processing_stage",
                "description": "Data processing stage",
                "steps": [
                    {
                        "name": "step1",
                        "description": "First step",
                        "function": "module.function",
                        "inputs": ["input"],
                        "outputs": ["result"],
                    },
                    {
                        "name": "step2",
                        "description": "Second step",
                        "function": "module.another_function",
                        "inputs": ["result"],  # Takes output from step1
                        "outputs": ["final"],
                    },
                ],
            }
        ],
        "steps": [
            {
                "name": "independent_step",
                "description": "Independent step that works with original input",
                "function": "module.process",
                "inputs": ["input"],
                "outputs": ["formatted"],
            }
        ],
    }

    try:
        path = Path(output_path)
        with open(path, "w") as f:
            if path.suffix in [".yaml", ".yml"]:
                yaml.dump(template, f, sort_keys=False)
            elif path.suffix == ".json":
                json.dump(template, f, indent=2)
            else:
                raise ValueError("Output file must be YAML or JSON")

        click.echo(f"Created template configuration at: {output_path}")
    except Exception as e:
        click.echo(f"Error creating template: {str(e)}", err=True)
        raise click.Abort()


@cli.command()
@click.argument("db_path", type=click.Path(exists=True))
def list(db_path: str) -> None:
    """List all pipelines in the database.

    DB_PATH should be the path to an existing SQLite database.
    """
    try:
        storage = SQLiteStorage(db_path)
        pipelines = storage.list_pipelines()

        if not pipelines:
            click.echo("No pipelines found in database.")
            return

        click.echo("\nPipelines:")
        for name, info in pipelines.items():
            click.echo(f"\n{name}:")
            click.echo(f"  State: {info['state']}")
            click.echo(f"  Created: {info['created_at']}")
            click.echo(f"  Updated: {info['updated_at']}")
    except Exception as e:
        click.echo(f"Error listing pipelines: {str(e)}", err=True)
        raise click.Abort()


@cli.command()
@click.argument("db_path", type=click.Path(exists=True))
@click.argument("pipeline_name", type=str)
def delete(db_path: str, pipeline_name: str) -> None:
    """Delete a pipeline from the database.

    DB_PATH should be the path to an existing SQLite database.
    PIPELINE_NAME should be the name of an existing pipeline.
    """
    try:
        if click.confirm(
            f"Are you sure you want to delete pipeline '{pipeline_name}'?"
        ):
            storage = SQLiteStorage(db_path)
            storage.delete_pipeline(pipeline_name)
            click.echo(f"Pipeline '{pipeline_name}' deleted successfully.")
    except Exception as e:
        click.echo(f"Error deleting pipeline: {str(e)}", err=True)
        raise click.Abort()


@cli.command()
@click.argument("db_path", type=click.Path(exists=True))
@click.argument("pipeline_name", type=str)
def info(db_path: str, pipeline_name: str) -> None:
    """Show detailed information about a specific pipeline.

    DB_PATH should be the path to an existing SQLite database.
    PIPELINE_NAME should be the name of an existing pipeline.
    """
    try:
        storage = SQLiteStorage(db_path)
        pipeline = storage.load_pipeline(pipeline_name)

        click.echo(f"\nPipeline: {pipeline.name}")
        click.echo(f"State: {pipeline.state.name}")

        if pipeline.stages:
            click.echo("\nStages:")
            for stage_name, stage in pipeline.stages.items():
                click.echo(f"\n  {stage_name}:")
                click.echo(f"    Description: {stage.description}")
                click.echo(f"    State: {stage.state.name}")
                click.echo(
                    f"    Dependencies: {', '.join(stage.dependencies) if stage.dependencies else 'None'}"
                )
                click.echo("    Steps:")
                for step in stage.steps:
                    click.echo(f"      - {step.name}")

        click.echo("\nSteps:")
        for step_name, step in pipeline.steps.items():
            click.echo(f"\n  {step_name}:")
            click.echo(f"    Description: {step.description}")
            click.echo(f"    State: {step.state.name}")
            click.echo(f"    Inputs: {', '.join(step.inputs)}")
            click.echo(f"    Outputs: {', '.join(step.outputs)}")
            click.echo(
                f"    Dependencies: {', '.join(step.dependencies) if step.dependencies else 'None'}"
            )

    except Exception as e:
        click.echo(f"Error getting pipeline info: {str(e)}", err=True)
        raise click.Abort()


def main() -> None:
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
