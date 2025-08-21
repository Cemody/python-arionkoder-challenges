"""
Example data processor plugins demonstrating metaclass contract enforcement.
"""

import json
import csv
import xml.etree.ElementTree as ET
from io import StringIO
from typing import Dict, Any
from utils import DataProcessorBase


class JSONProcessor(DataProcessorBase):
    """JSON data processor that conforms to DataProcessor contract"""
    
    processor_type = "json"
    version = "1.0.0"
    
    def _normalize(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively normalize a dict's keys (lowercase + underscores)."""
        normalized: Dict[str, Any] = {}
        for key, value in data.items():
            normalized_key = str(key).lower().replace(' ', '_')
            if isinstance(value, dict):
                normalized[normalized_key] = self._normalize(value)
            elif isinstance(value, list):
                normalized[normalized_key] = [
                    self._normalize(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                normalized[normalized_key] = value
        return normalized

    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process JSON-like data by normalizing and cleaning it.

        Nested dicts are normalized in-place (no metadata wrapping) to meet
        test expectations (e.g. processed['nested']['city']).
        """
        if not isinstance(data, dict):
            raise ValueError("JSON processor requires dictionary input")

        normalized = self._normalize(data)
        return {
            "processed_data": normalized,
            "original_keys": len(data),
            "processed_keys": len(normalized),
            "processor": self.processor_type
        }
    
    def validate_input(self, data: Dict[str, Any]) -> bool:
        """Validate that input is a valid dictionary"""
        return isinstance(data, dict)
    
    def get_schema(self) -> Dict[str, Any]:
        """Return schema for JSON processor"""
        return {
            "input": {
                "type": "object",
                "description": "Any valid JSON object/dictionary"
            },
            "output": {
                "type": "object",
                "properties": {
                    "processed_data": {"type": "object"},
                    "original_keys": {"type": "integer"},
                    "processed_keys": {"type": "integer"},
                    "processor": {"type": "string"}
                }
            }
        }


class CSVProcessor(DataProcessorBase):
    """CSV data processor that converts CSV strings to structured data"""
    
    processor_type = "csv"
    version = "1.0.0"
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process CSV data from input"""
        csv_content = data.get('csv_content', '')
        delimiter = data.get('delimiter', ',')
        
        if not isinstance(csv_content, str):
            raise ValueError("CSV processor requires 'csv_content' as string")
        
        # Parse CSV
        csv_reader = csv.DictReader(StringIO(csv_content), delimiter=delimiter)
        rows = list(csv_reader)
        
        # Convert numeric values
        processed_rows = []
        for row in rows:
            processed_row = {}
            for key, value in row.items():
                # Try to convert to number
                try:
                    if '.' in value:
                        processed_row[key] = float(value)
                    else:
                        processed_row[key] = int(value)
                except ValueError:
                    processed_row[key] = value
            processed_rows.append(processed_row)
        
        return {
            "processed_data": processed_rows,
            "row_count": len(processed_rows),
            "column_count": len(rows[0].keys()) if rows else 0,
            "columns": list(rows[0].keys()) if rows else [],
            "processor": self.processor_type
        }
    
    def validate_input(self, data: Dict[str, Any]) -> bool:
        """Validate CSV input"""
        return (
            isinstance(data, dict) and 
            'csv_content' in data and 
            isinstance(data['csv_content'], str)
        )
    
    def get_schema(self) -> Dict[str, Any]:
        """Return schema for CSV processor"""
        return {
            "input": {
                "type": "object",
                "required": ["csv_content"],
                "properties": {
                    "csv_content": {"type": "string"},
                    "delimiter": {"type": "string", "default": ","}
                }
            },
            "output": {
                "type": "object",
                "properties": {
                    "processed_data": {"type": "array"},
                    "row_count": {"type": "integer"},
                    "column_count": {"type": "integer"},
                    "columns": {"type": "array"},
                    "processor": {"type": "string"}
                }
            }
        }


class XMLProcessor(DataProcessorBase):
    """XML data processor that converts XML to structured data"""
    
    processor_type = "xml"
    version = "1.0.0"
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process XML data into structured format"""
        xml_content = data.get('xml_content', '')
        
        if not isinstance(xml_content, str):
            raise ValueError("XML processor requires 'xml_content' as string")
        
        try:
            root = ET.fromstring(xml_content)
            processed_data = self._xml_to_dict(root)
            
            return {
                "processed_data": processed_data,
                "root_tag": root.tag,
                "element_count": len(list(root.iter())),
                "processor": self.processor_type
            }
            
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML content: {e}")
    
    def _xml_to_dict(self, element):
        """Convert XML element to dictionary"""
        result = {}
        
        # Add attributes
        if element.attrib:
            result['@attributes'] = element.attrib
        
        # Add text content
        if element.text and element.text.strip():
            result['text'] = element.text.strip()
        
        # Add children
        children = {}
        for child in element:
            child_data = self._xml_to_dict(child)
            if child.tag in children:
                if not isinstance(children[child.tag], list):
                    children[child.tag] = [children[child.tag]]
                children[child.tag].append(child_data)
            else:
                children[child.tag] = child_data
        
        result.update(children)
        return result
    
    def validate_input(self, data: Dict[str, Any]) -> bool:
        """Validate XML input"""
        if not isinstance(data, dict) or 'xml_content' not in data:
            return False
        
        try:
            ET.fromstring(data['xml_content'])
            return True
        except ET.ParseError:
            return False
    
    def get_schema(self) -> Dict[str, Any]:
        """Return schema for XML processor"""
        return {
            "input": {
                "type": "object",
                "required": ["xml_content"],
                "properties": {
                    "xml_content": {"type": "string", "description": "Valid XML content"}
                }
            },
            "output": {
                "type": "object",
                "properties": {
                    "processed_data": {"type": "object"},
                    "root_tag": {"type": "string"},
                    "element_count": {"type": "integer"},
                    "processor": {"type": "string"}
                }
            }
        }
