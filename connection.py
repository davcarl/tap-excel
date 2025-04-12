import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
from datetime import datetime
import singer

class ExcelConnection:
    def __init__(self, config: Dict):
        self.config = config
        self.file_path = Path(config['file_path'])
        self.validate_config()
        
    def validate_config(self):
        """Validate configuration parameters"""
        if not self.file_path.exists():
            raise ValueError(f"File not found: {self.file_path}")
        
        self.chunk_size = self.config.get('chunk_size', 1000)
        if not isinstance(self.chunk_size, int) or self.chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer")
    
    def get_sheet_names(self) -> Set[str]:
        """Get all available sheet names"""
        with pd.ExcelFile(self.file_path, engine='openpyxl') as excel_file:
            return set(excel_file.sheet_names)
    
    def validate_requested_sheets(self, requested_sheets: Optional[List[str]]) -> Set[str]:
        """Validate which requested sheets exist"""
        available_sheets = self.get_sheet_names()
        
        if not requested_sheets:
            return available_sheets
            
        requested = set(requested_sheets)
        missing = requested - available_sheets
        if missing:
            singer.get_logger().warning(
                f"Missing sheets: {', '.join(missing)}. Available: {', '.join(available_sheets)}"
            )
        return requested & available_sheets
    
    def read_sheet_chunks(self, sheet_name: str):
        """Generator that yields chunks of a sheet"""
        return pd.read_excel(
            self.file_path,
            sheet_name=sheet_name,
            engine='openpyxl',
            parse_dates=True,
            keep_default_na=False,
            chunksize=self.chunk_size
        )
    
    def clean_record(self, record: Dict, float_precision: int = 2) -> Dict:
        """Convert pandas types to native Python types"""
        cleaned = {}
        for k, v in record.items():
            if pd.isna(v):
                cleaned[k] = None
            elif isinstance(v, pd.Timestamp):
                cleaned[k] = v.isoformat()
            elif isinstance(v, (pd.Timedelta, datetime)):
                cleaned[k] = str(v)
            elif isinstance(v, float):
                cleaned[k] = round(v, float_precision)
            else:
                cleaned[k] = v
        return cleaned
    
    def sync(self, state: Dict = None) -> Dict:
        """Main sync execution"""
        state = state or {}
        float_precision = self.config.get('float_precision', 2)
        sheets_to_sync = self.validate_requested_sheets(self.config.get('sheets'))
        
        for sheet_name in sheets_to_sync:
            singer.get_logger().info(f"Syncing sheet: {sheet_name}")
            
            for chunk in self.read_sheet_chunks(sheet_name):
                for _, row in chunk.iterrows():
                    record = self.clean_record(row.to_dict(), float_precision)
                    singer.write_record(sheet_name, record)
        
        return state