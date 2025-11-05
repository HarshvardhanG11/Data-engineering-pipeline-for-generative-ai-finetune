"""
Data validation module - validates data quality and format
"""
from typing import List, Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """Validates data quality and format"""
    
    def __init__(self, required_fields: List[str] = None, config: Dict[str, Any] = None):
        """
        Initialize data validator
        
        Args:
            required_fields: List of required fields
            config: Configuration dictionary
        """
        self.required_fields = required_fields or []
        self.config = config or {}
    
    def validate_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a single record
        
        Args:
            record: Data record to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check required fields
        for field in self.required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")
            elif not record[field] or (isinstance(record[field], str) and not record[field].strip()):
                errors.append(f"Empty required field: {field}")
        
        # Check for empty record
        if not record:
            errors.append("Record is empty")
        
        # Check for valid text content
        has_text = False
        for value in record.values():
            if isinstance(value, str) and value.strip():
                has_text = True
                break
            elif isinstance(value, list) and value:
                has_text = True
                break
            elif isinstance(value, dict) and value:
                has_text = True
                break
        
        if not has_text:
            errors.append("Record has no valid text content")
        
        return len(errors) == 0, errors
    
    def validate_dataset(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Validate entire dataset
        
        Args:
            data: List of data records
            
        Returns:
            Tuple of (valid_records, invalid_records_with_errors)
        """
        logger.info(f"Validating {len(data)} records...")
        
        valid_records = []
        invalid_records = []
        
        for idx, record in enumerate(data):
            is_valid, errors = self.validate_record(record)
            
            if is_valid:
                valid_records.append(record)
            else:
                invalid_records.append({
                    "record_index": idx,
                    "record": record,
                    "errors": errors
                })
                logger.debug(f"Record {idx} invalid: {errors}")
        
        logger.info(f"Validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid records")
        
        return valid_records, invalid_records
    
    def check_duplicates(self, data: List[Dict[str, Any]], key_field: str = "text") -> Tuple[List[Dict[str, Any]], int]:
        """
        Check for duplicate records
        
        Args:
            data: List of data records
            key_field: Field to use for duplicate detection
            
        Returns:
            Tuple of (unique_records, duplicate_count)
        """
        seen = set()
        unique_records = []
        duplicate_count = 0
        
        for record in data:
            key_value = str(record.get(key_field, ""))
            
            if key_value and key_value not in seen:
                seen.add(key_value)
                unique_records.append(record)
            else:
                duplicate_count += 1
        
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate records")
        
        return unique_records, duplicate_count
    
    def generate_quality_report(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate a quality report for the dataset
        
        Args:
            data: List of data records
            
        Returns:
            Quality report dictionary
        """
        logger.info("Generating quality report...")
        
        report = {
            "total_records": len(data),
            "fields": {},
            "text_length_stats": {},
            "validation": {}
        }
        
        if not data:
            return report
        
        # Analyze fields
        all_fields = set()
        for record in data:
            all_fields.update(record.keys())
        
        report["fields"]["total_unique_fields"] = len(all_fields)
        report["fields"]["field_names"] = list(all_fields)
        
        # Analyze text lengths
        text_lengths = []
        for record in data:
            for value in record.values():
                if isinstance(value, str):
                    text_lengths.append(len(value))
        
        if text_lengths:
            report["text_length_stats"] = {
                "min": min(text_lengths),
                "max": max(text_lengths),
                "avg": sum(text_lengths) / len(text_lengths),
                "median": sorted(text_lengths)[len(text_lengths) // 2]
            }
        
        # Run validation
        valid_records, invalid_records = self.validate_dataset(data)
        report["validation"] = {
            "valid_count": len(valid_records),
            "invalid_count": len(invalid_records),
            "validity_rate": len(valid_records) / len(data) if data else 0
        }
        
        # Check duplicates
        unique_records, duplicate_count = self.check_duplicates(data)
        report["validation"]["duplicate_count"] = duplicate_count
        report["validation"]["unique_count"] = len(unique_records)
        
        logger.info("Quality report generated")
        
        return report

