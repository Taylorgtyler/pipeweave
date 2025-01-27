import pytest
import json
import yaml
from pathlib import Path
from click.testing import CliRunner
from pipeweave.cli import (
    cli,
    load_config,
    load_data_file,
    save_results,
    import_function,
)
from pipeweave.core import Pipeline
from pipeweave.step import State
from pipeweave.storage import SQLiteStorage


@pytest.fixture
def runner():
    """Create a CLI runner for testing."""
    return CliRunner()


@pytest.fixture
def temp_config(tmp_path):
    """Create a temporary pipeline configuration file."""
    config = {
        "name": "test_pipeline",
        "description": "Test pipeline",
        "stages": [
            {
                "name": "stage1",
                "description": "Test stage",
                "steps": [
                    {
                        "name": "step1",
                        "description": "Test step 1",
                        "function": "tests.test_functions.double_number",
                        "inputs": ["input"],
                        "outputs": ["result"],
                    }
                ],
            }
        ],
        "steps": [
            {
                "name": "step2",
                "description": "Test step 2",
                "function": "tests.test_functions.add_one",
                "inputs": ["input"],
                "outputs": ["result"],
            }
        ],
    }

    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config, f)
    return config_file


@pytest.fixture
def temp_data(tmp_path):
    """Create a temporary input data file."""
    data = 5  # Just the number, not a dict
    data_file = tmp_path / "data.json"
    with open(data_file, "w") as f:
        json.dump(data, f)
    return data_file


def test_load_config(temp_config):
    """Test loading pipeline configuration from file."""
    config = load_config(str(temp_config))
    assert isinstance(config, dict)
    assert config["name"] == "test_pipeline"
    assert len(config["stages"]) == 1
    assert len(config["steps"]) == 1


def test_load_config_invalid_format():
    """Test loading configuration with invalid format."""
    with pytest.raises(ValueError):
        load_config("invalid.txt")


def test_load_config_not_found():
    """Test loading non-existent configuration file."""
    with pytest.raises(FileNotFoundError):
        load_config("nonexistent.yaml")


def test_load_data_file(temp_data):
    """Test loading input data from file."""
    data = load_data_file(str(temp_data))
    assert isinstance(data, int)
    assert data == 5


def test_load_data_file_invalid_format():
    """Test loading data with invalid format."""
    with pytest.raises(ValueError):
        load_data_file("invalid.txt")


def test_load_data_file_not_found():
    """Test loading non-existent data file."""
    with pytest.raises(FileNotFoundError):
        load_data_file("nonexistent.json")


def test_save_results(tmp_path):
    """Test saving pipeline results to file."""
    results = {"step1": {"result": 10}}
    output_file = tmp_path / "results.yaml"
    save_results(results, str(output_file))

    with open(output_file) as f:
        loaded = yaml.safe_load(f)
    assert loaded == results


def test_save_results_invalid_format(tmp_path):
    """Test saving results with invalid format."""
    results = {"step1": {"result": 10}}
    with pytest.raises(ValueError):
        save_results(results, str(tmp_path / "results.txt"))


def test_import_function():
    """Test importing function from module path."""
    # Create a temporary module with test functions
    module_dir = Path("tests")
    module_dir.mkdir(exist_ok=True)

    test_functions = module_dir / "test_functions.py"
    with open(test_functions, "w") as f:
        f.write(
            """
def double_number(x):
    return x * 2

def add_one(x):
    return x + 1
"""
        )

    # Test importing functions
    double_fn = import_function("tests.test_functions.double_number")
    assert callable(double_fn)
    assert double_fn(5) == 10

    add_fn = import_function("tests.test_functions.add_one")
    assert callable(add_fn)
    assert add_fn(5) == 6


def test_import_function_not_found():
    """Test importing non-existent function."""
    with pytest.raises(ImportError):
        import_function("nonexistent.module.function")


def test_cli_init_config(runner, tmp_path):
    """Test creating template configuration file."""
    output_file = tmp_path / "pipeline.yaml"
    result = runner.invoke(cli, ["init-config", str(output_file)])
    assert result.exit_code == 0
    assert output_file.exists()

    with open(output_file) as f:
        config = yaml.safe_load(f)
    assert "name" in config
    assert "stages" in config
    assert "steps" in config


def test_cli_run(runner, temp_config, temp_data, tmp_path):
    """Test running pipeline from configuration."""
    output_file = tmp_path / "results.json"
    result = runner.invoke(
        cli,
        [
            "run",
            str(temp_config),
            "--input-data",
            str(temp_data),
            "--output",
            str(output_file),
            "--no-save",
        ],
    )
    assert result.exit_code == 0
    assert output_file.exists()

    with open(output_file) as f:
        results = json.load(f)
    assert "step1" in results
    assert results["step1"]["result"] == 10  # double_number(5)
    assert "step2" in results
    assert results["step2"]["result"] == 11  # add_one(10)


def test_cli_run_invalid_config(runner):
    """Test running pipeline with invalid configuration."""
    result = runner.invoke(cli, ["run", "invalid.yaml"])
    assert result.exit_code != 0


def test_cli_list(runner, tmp_path):
    """Test listing pipelines in database."""
    db_file = tmp_path / "pipelines.db"

    # Create empty database file
    storage = SQLiteStorage(str(db_file))

    result = runner.invoke(cli, ["list", str(db_file)])
    assert "No pipelines found in database." in result.output


def test_cli_delete(runner, tmp_path):
    """Test deleting pipeline from database."""
    db_file = tmp_path / "pipelines.db"
    result = runner.invoke(cli, ["delete", str(db_file), "test_pipeline"], input="y\n")
    assert result.exit_code != 0  # Should fail as pipeline doesn't exist


def test_cli_info(runner, tmp_path):
    """Test showing pipeline information."""
    db_file = tmp_path / "pipelines.db"
    result = runner.invoke(cli, ["info", str(db_file), "test_pipeline"])
    assert result.exit_code != 0  # Should fail as pipeline doesn't exist
