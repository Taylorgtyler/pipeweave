from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from ..core import Pipeline

class StorageBackend(ABC):
    """Abstract base class for pipeline storage backends."""
    
    @abstractmethod
    def save_pipeline(self, pipeline: Pipeline) -> None:
        """Save a pipeline to storage.
        
        Args:
            pipeline (Pipeline): The pipeline to save
        """
        pass
    
    @abstractmethod
    def load_pipeline(self, pipeline_name: str) -> Pipeline:
        """Load a pipeline from storage.
        
        Args:
            pipeline_name (str): Name of the pipeline to load
            
        Returns:
            Pipeline: The loaded pipeline
            
        Raises:
            ValueError: If pipeline not found
        """
        pass
    
    @abstractmethod
    def delete_pipeline(self, pipeline_name: str) -> None:
        """Delete a pipeline from storage.
        
        Args:
            pipeline_name (str): Name of the pipeline to delete
            
        Raises:
            ValueError: If pipeline not found
        """
        pass
    
    @abstractmethod
    def list_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """List all stored pipelines.
        
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary of pipeline names and their metadata
        """
        pass 