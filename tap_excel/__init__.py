from singer_sdk import Tap, Stream
from singer_sdk.helpers._typing import TypeConformanceLevel
from singer_sdk.typing import PropertiesList, Property, StringType
from typing import List, Optional, Dict, Any
import pandas as pd
from xlsx2csv import Xlsx2csv
from io import StringIO
import openpyxl


class ExcelStream(Stream):
    def __init__(self, tap: Tap, name: str, schema: dict, records: List[Dict[str, Any]], **kwargs):
        super().__init__(tap=tap, name=name, schema=schema, **kwargs)
        self.records = records

    def get_records(self, context: Optional[dict] = None) -> List[Dict[str, Any]]:
        for record in self.records:
            yield record

class TapExcel(Tap):
    name = "tap-excel"
    
    def __init__(self, config: dict, **kwargs):
        """Initialize the TapExcel instance with the default singer-sdk logger.
        
        Args:
            config: Configuration dictionary.
            **kwargs: Additional keyword arguments (e.g., logger or other options).
        """
        super().__init__(config=config, **kwargs)  # singer-sdk will automatically handle the logger

    def get_sheet_names(self, **kwargs) -> List[str]:
        try:
            file_path = self.config["file_path"]
            excel_file = pd.ExcelFile(file_path, **kwargs)
            return excel_file.sheet_names  # Return the list of sheet names
        except Exception as e:
            self.logger.warning(f"Failed to get sheet names: {e}")
            return []

    def discover_streams(self, **kwargs) -> List[Stream]:
        """Discover and return the streams based on sheet structure."""
        streams = []
        sheet_structures: Dict[tuple, List[str]] = {}  # key = column tuple, value = list of sheet names
        sheet_dfs: Dict[str, pd.DataFrame] = {}        # stores individual DataFrames per sheet

        # Step 1: Get sheet names from the Excel file
        sheet_names = self.get_sheet_names(**kwargs)

        if not sheet_names:
            self.logger.warning("No sheets found in the Excel file.")
            return []

        # Step 2: Loop through all sheets and group them by their structure
        path = self.config["file_path"]
        for sheet in sheet_names:
            try:
                df = pd.read_excel(path, sheet_name=sheet, **kwargs)
                col_tuple = tuple(df.columns)
                sheet_dfs[sheet] = df  # Store the DataFrame by sheet name

                # Group sheets by column structure (headers)
                if col_tuple not in sheet_structures:
                    sheet_structures[col_tuple] = [sheet]
                else:
                    sheet_structures[col_tuple].append(sheet)
            except Exception as e:
                self.logger.warning(f"Failed to process sheet {sheet}: {e}")
                continue  # Skip failed sheet and continue with others

        # Step 3: Process each group of sheets based on structure
        for columns, sheets in sheet_structures.items():
            if len(sheets) == 1:
                # Unique structure — each sheet is its own stream
                sheet = sheets[0]
                df = sheet_dfs[sheet]
                records = df.to_dict(orient="records")
                schema = {
                    "type": "object",
                    "properties": {col: {"type": ["string", "null"]} for col in columns}
                }
                streams.append(ExcelStream(tap=self, name=sheet, schema=schema, records=records, **kwargs))
            else:
                # Grouped stream for shared structure
                dfs = [sheet_dfs[sheet] for sheet in sheets]
                merged_df = pd.concat(dfs, ignore_index=True)  # Merge DataFrames with matching structure
                records = merged_df.to_dict(orient="records")
                stream_name = "_".join(sheets)  # Example: Sheet1_Sheet3_Sheet7
                schema = {
                    "type": "object",
                    "properties": {col: {"type": ["string", "null"]} for col in columns}
                }
                streams.append(ExcelStream(tap=self, name=stream_name, schema=schema, records=records, **kwargs))

        # Return the list of discovered streams
        return streams

if __name__ == "__main__":
    TapExcel.cli()
cli = TapExcel.cli
