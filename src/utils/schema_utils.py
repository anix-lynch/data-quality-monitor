#!/usr/bin/env python3
"""
Data Quality & Monitoring Dashboard - Schema Validation Utilities

🎯 PURPOSE: Schema validation and constraint checking for data quality monitoring
📊 FEATURES: Required column validation, datatype checking, range constraints, non-null enforcement, JSON schema loading
🏗️ ARCHITECTURE: SchemaValidator class → JSON schema parsing → DataFrame validation → Structured error reporting
⚡ STATUS: Production-ready validation engine, handles complex schemas, fast constraint checking, comprehensive error categorization
"""

import json
from typing import Dict, Any, List, Optional
from pathlib import Path


class SchemaValidator:
    """Handles schema validation logic for data quality monitoring."""

    def __init__(self, schema_path: str):
        self.schema_path = Path(schema_path)
        self.schema = self._load_schema()

    def _load_schema(self) -> Dict[str, Any]:
        """Load schema from JSON file."""
        with open(self.schema_path, 'r') as f:
            return json.load(f)

    def validate_required_columns(self, df_columns: List[str]) -> Dict[str, Any]:
        """Check for required columns."""
        required_fields = [field for field, config in self.schema['fields'].items()
                          if config.get('required', False)]
        missing_columns = [col for col in required_fields if col not in df_columns]

        return {
            'missing_required_columns': missing_columns,
            'has_all_required': len(missing_columns) == 0
        }

    def validate_datatypes(self, df_dtypes: Dict[str, str]) -> Dict[str, Any]:
        """Validate column datatypes against schema."""
        dtype_issues = {}

        for field_name, field_config in self.schema['fields'].items():
            if field_name in df_dtypes:
                expected_dtype = field_config['dtype']
                actual_dtype = df_dtypes[field_name]

                # Simple dtype mapping (could be expanded)
                dtype_mapping = {
                    'int': ['int64', 'int32', 'int16', 'int8'],
                    'float': ['float64', 'float32'],
                    'string': ['object', 'string'],
                    'datetime': ['datetime64[ns]']
                }

                if expected_dtype in dtype_mapping:
                    if actual_dtype not in dtype_mapping[expected_dtype]:
                        dtype_issues[field_name] = {
                            'expected': expected_dtype,
                            'actual': actual_dtype
                        }

        return {
            'dtype_issues': dtype_issues,
            'has_dtype_issues': len(dtype_issues) > 0
        }

    def validate_constraints(self, df) -> Dict[str, Any]:
        """Validate data constraints (ranges, allowed values, etc.)."""
        constraint_violations = {}

        for field_name, field_config in self.schema['fields'].items():
            if field_name not in df.columns:
                continue

            column_data = df[field_name]

            # Numeric range constraints
            if 'min' in field_config and 'max' in field_config:
                min_val, max_val = field_config['min'], field_config['max']
                if column_data.dtype in ['int64', 'float64']:
                    out_of_range = column_data[(column_data < min_val) | (column_data > max_val)]
                    if len(out_of_range) > 0:
                        constraint_violations[field_name] = {
                            'constraint': f'range_{min_val}_{max_val}',
                            'violations': len(out_of_range),
                            'min_violation': float(out_of_range.min()) if len(out_of_range) > 0 else None,
                            'max_violation': float(out_of_range.max()) if len(out_of_range) > 0 else None
                        }

            # Non-null expectations
            if field_config.get('required', False):
                null_count = column_data.isnull().sum()
                if null_count > 0:
                    constraint_violations[field_name] = constraint_violations.get(field_name, {})
                    constraint_violations[field_name]['null_violations'] = null_count

        return {
            'constraint_violations': constraint_violations,
            'has_violations': len(constraint_violations) > 0
        }
