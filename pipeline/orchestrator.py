"""
Main pipeline orchestrator - coordinates all pipeline stages
"""
import json
import jsonlines
from pathlib import Path
from typing import List, Dict, Any, Tuple
import logging
import random

from src.data_ingestion.ingestor import DataIngestor
from src.data_cleaning.cleaner import DataCleaner
from src.data_transformation.transformer import DataTransformer
from src.data_validation.validator import DataValidator
from src.utils.logger import setup_logger
from src.utils.config_loader import load_config

logger = setup_logger(__name__)

class PipelineOrchestrator:
    """Main pipeline orchestrator"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize pipeline orchestrator
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        
        # Initialize components
        self.ingestor = DataIngestor(
            self.config['pipeline']['supported_formats']
        )
        
        self.cleaner = DataCleaner(
            min_length=self.config['pipeline']['min_text_length'],
            max_length=self.config['pipeline']['max_text_length']
        )
        
        self.transformer = DataTransformer(
            output_format=self.config['transformation']['output_format'],
            config=self.config.get('transformation', {})
        )
        
        self.validator = DataValidator(
            required_fields=self.config['validation']['required_fields']
        )
    
    def run(self, input_path: str, output_path: str = None) -> Dict[str, Any]:
        """
        Run the complete data pipeline
        
        Args:
            input_path: Path to input data file or directory
            output_path: Optional output path
            
        Returns:
            Pipeline execution report
        """
        logger.info("=" * 80)
        logger.info("Starting Data Engineering Pipeline for Generative AI Fine-tuning")
        logger.info("=" * 80)
        
        report = {
            "stages": {},
            "final_stats": {}
        }
        
        try:
            # Stage 1: Data Ingestion
            logger.info("\n[Stage 1] Data Ingestion")
            logger.info("-" * 80)
            
            input_path_obj = Path(input_path)
            if input_path_obj.is_file():
                data = self.ingestor.load_data(input_path)
            else:
                data = self.ingestor.load_from_directory(input_path)
            
            report["stages"]["ingestion"] = {
                "records_loaded": len(data),
                "status": "success"
            }
            logger.info(f"✓ Loaded {len(data)} records")
            
            # Stage 2: Data Cleaning
            logger.info("\n[Stage 2] Data Cleaning")
            logger.info("-" * 80)
            
            data = self.cleaner.clean_dataset(data)
            
            if self.config['validation'].get('check_duplicates', True):
                data = self.cleaner.remove_duplicates(data)
            
            if self.config['validation'].get('check_empty_fields', True):
                required_fields = self.config['validation'].get('required_fields', [])
                if required_fields:
                    data = self.cleaner.remove_empty_fields(data, required_fields)
            
            report["stages"]["cleaning"] = {
                "records_after_cleaning": len(data),
                "status": "success"
            }
            logger.info(f"✓ Cleaned dataset: {len(data)} records")
            
            # Stage 3: Data Transformation
            logger.info("\n[Stage 3] Data Transformation")
            logger.info("-" * 80)
            
            data = self.transformer.transform_dataset(data)
            
            report["stages"]["transformation"] = {
                "records_transformed": len(data),
                "output_format": self.config['transformation']['output_format'],
                "status": "success"
            }
            logger.info(f"✓ Transformed to {self.config['transformation']['output_format']} format")
            
            # Stage 4: Data Validation
            logger.info("\n[Stage 4] Data Validation")
            logger.info("-" * 80)
            
            valid_data, invalid_data = self.validator.validate_dataset(data)
            
            report["stages"]["validation"] = {
                "valid_records": len(valid_data),
                "invalid_records": len(invalid_data),
                "status": "success"
            }
            logger.info(f"✓ Validated: {len(valid_data)} valid records")
            
            # Generate quality report
            quality_report = self.validator.generate_quality_report(valid_data)
            report["quality_report"] = quality_report
            
            # Stage 5: Save Output
            logger.info("\n[Stage 5] Saving Output")
            logger.info("-" * 80)
            
            if not output_path:
                output_path = self.config['data']['output_data_dir']
            
            output_path_obj = Path(output_path)
            output_path_obj.mkdir(parents=True, exist_ok=True)
            
            # Split data if needed
            train_data, val_data = self._split_data(valid_data)
            
            # Save datasets
            train_file = output_path_obj / "train.jsonl"
            val_file = output_path_obj / "val.jsonl"
            
            self._save_jsonl(train_data, train_file)
            self._save_jsonl(val_data, val_file)
            
            logger.info(f"✓ Saved {len(train_data)} training records to {train_file}")
            logger.info(f"✓ Saved {len(val_data)} validation records to {val_file}")
            
            report["stages"]["output"] = {
                "train_records": len(train_data),
                "val_records": len(val_data),
                "train_file": str(train_file),
                "val_file": str(val_file),
                "status": "success"
            }
            
            # Final stats
            report["final_stats"] = {
                "total_processed": len(data),
                "total_valid": len(valid_data),
                "total_invalid": len(invalid_data),
                "train_size": len(train_data),
                "val_size": len(val_data)
            }
            
            logger.info("\n" + "=" * 80)
            logger.info("Pipeline Execution Complete!")
            logger.info("=" * 80)
            logger.info(f"Total records processed: {len(data)}")
            logger.info(f"Valid records: {len(valid_data)}")
            logger.info(f"Training set: {len(train_data)}")
            logger.info(f"Validation set: {len(val_data)}")
            
            return report
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            report["error"] = str(e)
            report["status"] = "failed"
            raise
    
    def _split_data(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Split data into train and validation sets
        
        Args:
            data: List of data records
            
        Returns:
            Tuple of (train_data, val_data)
        """
        output_config = self.config['output']
        train_split = output_config['train_split']
        val_split = output_config['val_split']
        
        if output_config.get('shuffle', True):
            random.seed(output_config.get('seed', 42))
            random.shuffle(data)
        
        split_idx = int(len(data) * train_split)
        train_data = data[:split_idx]
        val_data = data[split_idx:split_idx + int(len(data) * val_split)]
        
        return train_data, val_data
    
    def _save_jsonl(self, data: List[Dict[str, Any]], file_path: Path):
        """
        Save data to JSONL file
        
        Args:
            data: List of data records
            file_path: Output file path
        """
        with jsonlines.open(file_path, mode='w') as writer:
            for record in data:
                writer.write(record)

