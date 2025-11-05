"""
Data transformation module - converts data to formats suitable for AI fine-tuning
"""
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class DataTransformer:
    """Transforms data into formats suitable for generative AI fine-tuning"""
    
    def __init__(self, output_format: str = "instruction", config: Dict[str, Any] = None):
        """
        Initialize data transformer
        
        Args:
            output_format: Output format (instruction, conversation, completion)
            config: Configuration dictionary
        """
        self.output_format = output_format
        self.config = config or {}
        
        # Get format-specific templates
        if output_format == "instruction":
            self.template = self.config.get("instruction_template", {})
        elif output_format == "conversation":
            self.template = self.config.get("conversation_template", {})
        else:
            self.template = {}
    
    def transform_to_instruction_format(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform record to instruction format
        
        Format:
        {
            "instruction": "...",
            "input": "...",  # optional
            "response": "..."
        }
        """
        system_prompt = self.template.get("system_prompt", "You are a helpful AI assistant.")
        instruction_prefix = self.template.get("instruction_prefix", "### Instruction:\n")
        response_prefix = self.template.get("response_prefix", "### Response:\n")
        
        # Try to extract instruction and response from record
        instruction = record.get("instruction") or record.get("prompt") or record.get("question") or record.get("input", "")
        response = record.get("response") or record.get("output") or record.get("answer") or record.get("text", "")
        input_text = record.get("context") or record.get("input_context", "")
        
        # Build formatted text
        formatted_text = f"{system_prompt}\n\n"
        if input_text:
            formatted_text += f"Input: {input_text}\n\n"
        formatted_text += f"{instruction_prefix}{instruction}\n\n{response_prefix}{response}"
        
        return {
            "instruction": instruction,
            "input": input_text if input_text else None,
            "response": response,
            "text": formatted_text
        }
    
    def transform_to_conversation_format(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform record to conversation format
        
        Format:
        {
            "messages": [
                {"role": "system", "content": "..."},
                {"role": "user", "content": "..."},
                {"role": "assistant", "content": "..."}
            ]
        }
        """
        system_prompt = self.template.get("system_prompt", "You are a helpful AI assistant.")
        user_prefix = self.template.get("user_prefix", "User: ")
        assistant_prefix = self.template.get("assistant_prefix", "Assistant: ")
        
        messages = [
            {"role": "system", "content": system_prompt}
        ]
        
        # Extract user and assistant messages
        user_content = record.get("user") or record.get("instruction") or record.get("prompt") or record.get("question", "")
        assistant_content = record.get("assistant") or record.get("response") or record.get("output") or record.get("answer", "")
        
        if user_content:
            messages.append({"role": "user", "content": user_content})
        
        if assistant_content:
            messages.append({"role": "assistant", "content": assistant_content})
        
        # Build formatted text
        formatted_text = f"{system_prompt}\n\n"
        if user_content:
            formatted_text += f"{user_prefix}{user_content}\n\n"
        if assistant_content:
            formatted_text += f"{assistant_prefix}{assistant_content}"
        
        return {
            "messages": messages,
            "text": formatted_text
        }
    
    def transform_to_completion_format(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform record to completion format (simple text continuation)
        
        Format:
        {
            "prompt": "...",
            "completion": "..."
        }
        """
        prompt = record.get("prompt") or record.get("instruction") or record.get("input", "")
        completion = record.get("completion") or record.get("response") or record.get("output") or record.get("text", "")
        
        formatted_text = f"{prompt}{completion}"
        
        return {
            "prompt": prompt,
            "completion": completion,
            "text": formatted_text
        }
    
    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a single record based on output format
        
        Args:
            record: Data record to transform
            
        Returns:
            Transformed record
        """
        if self.output_format == "instruction":
            return self.transform_to_instruction_format(record)
        elif self.output_format == "conversation":
            return self.transform_to_conversation_format(record)
        elif self.output_format == "completion":
            return self.transform_to_completion_format(record)
        else:
            logger.warning(f"Unknown format: {self.output_format}, returning record as-is")
            return record
    
    def transform_dataset(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform entire dataset
        
        Args:
            data: List of data records
            
        Returns:
            Transformed dataset
        """
        logger.info(f"Transforming {len(data)} records to {self.output_format} format...")
        
        transformed_data = []
        
        for record in data:
            try:
                transformed_record = self.transform_record(record)
                transformed_data.append(transformed_record)
            except Exception as e:
                logger.error(f"Error transforming record: {e}")
                continue
        
        logger.info(f"Transformed {len(transformed_data)} records successfully")
        
        return transformed_data

