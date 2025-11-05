"""
Configuration loader utility
"""
import yaml
import os
from pathlib import Path

def load_config(config_path: str = "config.yaml") -> dict:
    """
    Load configuration from YAML file
    
    Args:
        config_path: Path to config file
        
    Returns:
        Configuration dictionary
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Create directories if they don't exist
    for dir_path in [
        config['data']['raw_data_dir'],
        config['data']['processed_data_dir'],
        config['data']['output_data_dir'],
        config['data']['sample_data_dir']
    ]:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    return config

