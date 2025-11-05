"""
Data cleaning and preprocessing module
"""
import re
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DataCleaner:
    """Handles data cleaning and preprocessing"""
    
    def __init__(self, min_length: int = 10, max_length: int = 5000):
        """
        Initialize data cleaner
        
        Args:
            min_length: Minimum text length
            max_length: Maximum text length
        """
        self.min_length = min_length
        self.max_length = max_length
    
    def clean_text(self, text: str) -> str:
        """
        Clean a single text string
        
        Args:
            text: Raw text to clean
            
        Returns:
            Cleaned text
        """
        if not isinstance(text, str):
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters (keep basic punctuation)
        text = re.sub(r'[^\w\s\.\,\!\?\;\:\-\(\)]', '', text)
        
        # Strip whitespace
        text = text.strip()
        
        return text
    
    def clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean a single data record
        
        Args:
            record: Data record to clean
            
        Returns:
            Cleaned record
        """
        cleaned = {}
        
        for key, value in record.items():
            if isinstance(value, str):
                cleaned[key] = self.clean_text(value)
            elif isinstance(value, (int, float, bool)):
                cleaned[key] = value
            elif isinstance(value, list):
                cleaned[key] = [self.clean_text(str(item)) if isinstance(item, str) else item 
                               for item in value]
            elif isinstance(value, dict):
                cleaned[key] = self.clean_record(value)
            else:
                cleaned[key] = value
        
        return cleaned
    
    def clean_dataset(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean entire dataset
        
        Args:
            data: List of data records
            
        Returns:
            Cleaned dataset
        """
        logger.info(f"Cleaning {len(data)} records...")
        
        cleaned_data = []
        removed_count = 0
        
        for record in data:
            cleaned_record = self.clean_record(record)
            
            # Check if record has valid text content
            has_valid_text = False
            for value in cleaned_record.values():
                if isinstance(value, str) and self.min_length <= len(value) <= self.max_length:
                    has_valid_text = True
                    break
            
            if has_valid_text:
                cleaned_data.append(cleaned_record)
            else:
                removed_count += 1
        
        logger.info(f"Cleaned {len(cleaned_data)} records (removed {removed_count} invalid records)")
        
        return cleaned_data
    
    def remove_duplicates(self, data: List[Dict[str, Any]], key_fields: List[str] = None) -> List[Dict[str, Any]]:
        """
        Remove duplicate records
        
        Args:
            data: List of data records
            key_fields: Fields to use for duplicate detection
            
        Returns:
            Dataset without duplicates
        """
        if not key_fields:
            # Use all string fields for duplicate detection
            key_fields = []
            if data:
                for key, value in data[0].items():
                    if isinstance(value, str):
                        key_fields.append(key)
        
        if not key_fields:
            logger.warning("No key fields specified, skipping duplicate removal")
            return data
        
        logger.info(f"Removing duplicates based on fields: {key_fields}")
        
        seen = set()
        unique_data = []
        duplicate_count = 0
        
        for record in data:
            # Create a tuple of key field values
            key_tuple = tuple(str(record.get(field, "")) for field in key_fields)
            
            if key_tuple not in seen:
                seen.add(key_tuple)
                unique_data.append(record)
            else:
                duplicate_count += 1
        
        logger.info(f"Removed {duplicate_count} duplicates. {len(unique_data)} unique records remaining")
        
        return unique_data
    
    def remove_empty_fields(self, data: List[Dict[str, Any]], required_fields: List[str] = None) -> List[Dict[str, Any]]:
        """
        Remove records with empty required fields
        
        Args:
            data: List of data records
            required_fields: Fields that must not be empty
            
        Returns:
            Dataset with only records that have required fields
        """
        if not required_fields:
            return data
        
        logger.info(f"Removing records with empty required fields: {required_fields}")
        
        filtered_data = []
        removed_count = 0
        
        for record in data:
            has_all_fields = all(
                record.get(field) and str(record.get(field)).strip()
                for field in required_fields
            )
            
            if has_all_fields:
                filtered_data.append(record)
            else:
                removed_count += 1
        
        logger.info(f"Removed {removed_count} records with empty required fields. {len(filtered_data)} records remaining")
        
        return filtered_data

