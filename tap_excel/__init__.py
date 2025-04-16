from singer_sdk import Tap, Stream
from singer_sdk.typing import PropertiesList, Property, StringType
from typing import List, Optional, Dict, Any, Generator
import pandas as pd


class ExcelStream(Stream):
    def __init__(
        self,
        tap: Tap,
        name: str,
        schema: dict,
        records: List[Dict[str, Any]],
        replication_key: Optional[str] = None,
        **kwargs
    ):
        super().__init__(tap=tap, name=name, schema=schema, **kwargs)
        self.records = records
        self._replication_key = replication_key

    @property
    def replication_key(self) -> Optional[str]:
        return self._replication_key

    def get_records(self, context: Optional[dict]) -> Generator[Dict[str, Any], None, None]:
        for record in self.records:
            yield record


class TapExcel(Tap):
    name = "tap-excel"
    config_jsonschema = {
        "type": "object",
        "properties": {
            "file_path": {"type": "string"},
            "sheets": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "replication_key": {"type": "string", "description": "Optional key for incremental sync"}
                    },
                    "required": ["name"]
                },
                "description": "List of sheet configurations, optional"
            }
        },
        "required": ["file_path"]
    }

    def discover_streams(self) -> List[Stream]:
        file_path = self.config["file_path"]
        sheets_config = self.config.get("sheets", [])
        streams = []

        try:
            excel_file = pd.ExcelFile(file_path)
        except Exception as e:
            self.logger.error(f"Could not read Excel file: {e}")
            return []

        # If sheets are undefined, use all available sheets in the file
        sheet_names = excel_file.sheet_names
        if not sheets_config:
            self.logger.info("No specific sheets provided, using all sheets in the file.")
            sheets_config = [{"name": sheet} for sheet in sheet_names]

        for sheet_cfg in sheets_config:
            sheet_name = sheet_cfg["name"]
            # Use the sheet-specific replication_key
            replication_key = sheet_cfg.get("replication_key")

            if sheet_name not in sheet_names:
                self.logger.warning(f"Sheet '{sheet_name}' not found in file. Skipping.")
                continue

            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)

                # If replication_key is defined, check if the column exists
                if replication_key:
                    if replication_key not in df.columns:
                        self.logger.error(f"Replication key column '{replication_key}' is missing from sheet '{sheet_name}'. Failing sync.")
                        raise ValueError(f"Replication key column '{replication_key}' is missing from sheet '{sheet_name}'.")

                records = df.to_dict(orient="records")
                schema = {
                    "type": "object",
                    "properties": {
                        col: {"type": ["string", "null"]} for col in df.columns
                    }
                }

                streams.append(
                    ExcelStream(
                        tap=self,
                        name=sheet_name,
                        schema=schema,
                        records=records,
                        replication_key=replication_key  # Will be None if not available
                    )
                )
            except Exception as e:
                self.logger.warning(f"Failed to load sheet '{sheet_name}': {e}")
                continue

        return streams


if __name__ == "__main__":
    TapExcel.cli()

cli = TapExcel.cli
