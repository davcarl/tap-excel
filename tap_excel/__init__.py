import sys
import json
import singer
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

LOGGER = singer.get_logger()

class ExcelTap():
    def __init__(self, config: Dict):
        self.config = config
        self.file_path = Path(config['file_path'])
        self.sheets_to_sync = config.get('sheets', None)
        
        if not self.file_path.exists():
            LOGGER.error(f"File not found: {self.file_path}")
            sys.exit(1)

    def _get_available_sheets(self) -> List[str]:
        """Get list of all available sheets in the Excel file"""
        try:
            with pd.ExcelFile(self.file_path, engine='openpyxl') as xls:
                return xls.sheet_names
        except Exception as e:
            LOGGER.error(f"Error reading Excel file: {str(e)}")
            sys.exit(1)

    def _infer_column_type(self, series: pd.Series, sample_size: int = 100) -> Dict:
        """Enhanced column type inference with metadata-style detection"""
        # Get basic type from pandas
        col_type = str(series.dtype)
        
        # Handle null-only columns
        if series.isna().all():
            return {"type": ["null", "string"]}
        
        # Get sample of non-null values
        sample = series.dropna().head(sample_size)
        
        # Initialize type info
        type_info = {
            "type": ["null"],
            "metadata": {
                "excel_dtype": col_type,
                "inferred_type": None
            }
        }
        
        # Check for datetime values
        if pd.api.types.is_datetime64_any_dtype(series):
            type_info["type"].append("string")
            type_info["format"] = "date-time"
            type_info["metadata"]["inferred_type"] = "datetime"
            return type_info
        
        # Check for boolean values
        if col_type == 'bool' or sample.astype(str).str.lower().isin(['true', 'false']).all():
            type_info["type"].append("boolean")
            type_info["metadata"]["inferred_type"] = "boolean"
            return type_info
        
        # Numeric type detection
        numeric_types = []
        if pd.api.types.is_numeric_dtype(series):
            # Check if all numbers are integers
            if (series.dropna() % 1 == 0).all():
                numeric_types.append("integer")
                type_info["metadata"]["inferred_type"] = "integer"
            else:
                numeric_types.append("number")
                type_info["metadata"]["inferred_type"] = "number"
        
        # String type detection with format checking
        str_sample = sample.astype(str)
        
        # Date format detection
        date_formats = [
            (r'\d{4}-\d{2}-\d{2}', 'date'),          # YYYY-MM-DD
            (r'\d{2}/\d{2}/\d{4}', 'date'),          # MM/DD/YYYY
            (r'\d{4}/\d{2}/\d{2}', 'date'),          # YYYY/MM/DD
            (r'\d{2}-\d{2}-\d{4}', 'date'),          # MM-DD-YYYY
            (r'\d{4}\.\d{2}\.\d{2}', 'date'),        # YYYY.MM.DD
            (r'\d{2}:\d{2}:\d{2}', 'time'),          # HH:MM:SS
            (r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', 'date-time')  # YYYY-MM-DD HH:MM:SS
        ]
        
        for pattern, fmt in date_formats:
            if str_sample.str.match(pattern).any():
                type_info["type"].append("string")
                type_info["format"] = fmt
                type_info["metadata"]["inferred_type"] = fmt
                return type_info
        
        # Currency detection
        if str_sample.str.match(r'^[£$€¥]\s?\d+\.?\d*$').any():
            type_info["type"].append("string")
            type_info["metadata"]["inferred_type"] = "currency"
            return type_info
        
        # If we have numeric types, use them
        if numeric_types:
            type_info["type"].extend(numeric_types)
            # Add string as fallback
            type_info["type"].append("string")
            return type_info
        
        # Default to string
        type_info["type"].append("string")
        type_info["metadata"]["inferred_type"] = "string"
        return type_info

    def _infer_schema(self, df: pd.DataFrame) -> Dict:
        """Infer schema with enhanced metadata-driven type detection"""
        properties = {}
        
        for col in df.columns:
            properties[col] = self._infer_column_type(df[col])
            
            # Convert detected string dates to datetime for consistent processing
            if properties[col].get('format') in ('date', 'date-time', 'time'):
                df[col] = pd.to_datetime(df[col], errors='ignore')
        
        return {
            "type": "object",
            "properties": properties
        }

    def _clean_record(self, record: Dict) -> Dict:
        """Clean record by converting pandas types to native Python types"""
        cleaned = {}
        for k, v in record.items():
            if pd.isna(v):
                cleaned[k] = None
            elif isinstance(v, pd.Timestamp):
                cleaned[k] = v.isoformat()
            elif isinstance(v, (pd.Timedelta, datetime)):
                cleaned[k] = str(v)
            elif isinstance(v, float):
                cleaned[k] = round(v, self.float_precision)
            else:
                cleaned[k] = v
        return cleaned

    def discover(self) -> List[Dict]:
        """Discover available streams (sheets) and their schemas"""
        available_sheets = self._get_available_sheets()
        
        # Log all discovered sheets if no specific sheets were requested
        if not self.sheets_to_sync:
            LOGGER.info(f"Discovered sheets: {available_sheets}")
        else:
            LOGGER.info(f"Requested sheets: {self.sheets_to_sync}")
            
        streams = []
        found_sheets = []
        
        for sheet_name in (self.sheets_to_sync if self.sheets_to_sync else available_sheets):
            if sheet_name not in available_sheets:
                LOGGER.warning(f"Sheet not found: '{sheet_name}' - skipping")
                continue
                
            found_sheets.append(sheet_name)
            try:
                df = pd.read_excel(
                    self.file_path,
                    sheet_name=sheet_name,
                    engine='openpyxl'
                )
            except Exception as e:
                LOGGER.error(f"Error reading sheet '{sheet_name}': {str(e)}")
                continue

            schema = self._infer_schema(df)
            metadata = singer.metadata.get_standard_metadata(
                schema=schema,
                key_properties=self.config.get('key_properties', []),
                replication_method=self.config.get('replication_method', 'FULL_TABLE')
            )

            streams.append({
                "stream": sheet_name,
                "tap_stream_id": sheet_name,
                "schema": schema,
                "metadata": metadata
            })

        # Log summary of found sheets
        if not found_sheets:
            LOGGER.warning("No sheets found to process")
        else:
            LOGGER.info(f"Processing sheets: {found_sheets}")

        return streams

    def sync(self, state: Optional[Dict] = None) -> Dict:
        """Sync data from Excel sheets"""
        state = state or {}
        bookmark_column = self.config.get('bookmark_column')
        
        # First get available sheets
        available_sheets = self._get_available_sheets()
        
        # Determine which sheets to process
        if self.sheets_to_sync:
            sheets_to_process = [s for s in self.sheets_to_sync if s in available_sheets]
            missing_sheets = set(self.sheets_to_sync) - set(available_sheets)
            if missing_sheets:
                LOGGER.warning(f"Requested sheets not found: {missing_sheets}")
        else:
            sheets_to_process = available_sheets
            
        if not sheets_to_process:
            LOGGER.warning("No sheets available to process")
            return state
            
        LOGGER.info(f"Processing sheets: {sheets_to_process}")

        # Process each sheet
        for sheet_name in sheets_to_process:
            try:
                df = pd.read_excel(
                    self.file_path,
                    sheet_name=sheet_name,
                    engine='openpyxl',
                    parse_dates=True,
                    keep_default_na=False
                )
            except Exception as e:
                LOGGER.error(f"Error reading sheet '{sheet_name}': {str(e)}")
                continue

            # Write schema
            schema = self._infer_schema(df)
            singer.write_schema(
                sheet_name,
                schema,
                self.config.get('key_properties', [])
            )

            # Get bookmark value if incremental
            max_bookmark = None
            if bookmark_column and bookmark_column in df.columns:
                max_bookmark = df[bookmark_column].max()

            # Process records
            records_written = 0
            for _, row in df.iterrows():
                record = self._clean_record(row.to_dict())
                
                # Skip based on bookmark for incremental sync
                if (bookmark_column and 
                    record.get(bookmark_column) and 
                    record[bookmark_column] <= state.get(sheet_name, {}).get('bookmark')):
                    continue
                    
                singer.write_record(sheet_name, record)
                records_written += 1

            LOGGER.info(f"Wrote {records_written} records for sheet {sheet_name}")
            
            # Update state if incremental
            if max_bookmark:
                state = singer.write_bookmark(
                    state,
                    sheet_name,
                    'bookmark',
                    max_bookmark
                )

        return state

def main():
    try:
        config = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        LOGGER.error(f"Invalid JSON config: {str(e)}")
        sys.exit(1)

    tap = ExcelTap(config)
    
    # Check if discovery mode
    if len(sys.argv) > 1 and sys.argv[1] == '--discover':
        catalog = {"streams": tap.discover()}
        print(json.dumps(catalog, indent=2))
    else:
        # Normal sync mode
        state = {}
        if config.get('state'):
            try:
                state = json.loads(config['state'])
            except json.JSONDecodeError as e:
                LOGGER.warning(f"Invalid state: {str(e)}")
        
        tap.sync(state)

if __name__ == "__main__":
    main()
