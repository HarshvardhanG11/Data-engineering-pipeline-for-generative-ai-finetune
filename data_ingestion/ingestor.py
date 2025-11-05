"""
Data ingestion module - loads data from various sources
"""
import pandas as pd
import json
import jsonlines
from pathlib import Path
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DataIngestor:
    """Handles data ingestion from various file formats"""
    
    def __init__(self, supported_formats: List[str] = None):
        """
        Initialize data ingestor
        
        Args:
            supported_formats: List of supported file formats
        """
        self.supported_formats = supported_formats or ["json", "jsonl", "csv", "txt"]
    
    def load_data(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Load data from file based on format
        
        Args:
            file_path: Path to data file
            
        Returns:
            List of data records
        """
        file_path = Path(file_path)
        file_extension = file_path.suffix.lower().lstrip('.')
        
        if file_extension not in self.supported_formats:
            raise ValueError(f"Unsupported file format: {file_extension}")
        
        logger.info(f"Loading data from {file_path}")
        
        if file_extension == "json":
            return self._load_json(file_path)
        elif file_extension == "jsonl":
            return self._load_jsonl(file_path)
        elif file_extension == "csv":
            return self._load_csv(file_path)
        elif file_extension == "txt":
            return self._load_txt(file_path)
    
    def _load_json(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load JSON file"""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # If it's a list, return directly; if it's a dict, convert to list
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        else:
            raise ValueError("Invalid JSON format")
    
    def _load_jsonl(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load JSONL file"""
        data = []
        with jsonlines.open(file_path) as reader:
            for obj in reader:
                data.append(obj)
        return data
    
    def _load_csv(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load CSV file"""
        df = pd.read_csv(file_path)
        return df.to_dict('records')
    
    def _load_txt(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load text file (one record per line)"""
        data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    data.append({
                        "text": line,
                        "line_number": line_num
                    })
        return data
    
    def load_from_directory(self, directory: str, pattern: str = "*") -> List[Dict[str, Any]]:
        """
        Load all matching files from a directory
        
        Args:
            directory: Directory path
            pattern: File pattern to match
            
        Returns:
            Combined list of all records
        """
        directory = Path(directory)
        all_data = []
        
        for file_path in directory.glob(pattern):
            if file_path.is_file():
                try:
                    data = self.load_data(str(file_path))
                    all_data.extend(data)
                    logger.info(f"Loaded {len(data)} records from {file_path.name}")
                except Exception as e:
                    logger.error(f"Error loading {file_path}: {e}")
        
        logger.info(f"Total records loaded: {len(all_data)}")
        return all_data

