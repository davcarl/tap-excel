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

        sheet_names = excel_file.sheet_names
        if not sheets_config:
            self.logger.info("No specific sheets provided, using all sheets in the file.")
            sheets_config = [{"name": sheet} for sheet in sheet_names]

        for sheet_cfg in sheets_config:
            sheet_name = sheet_cfg["name"]
            replication_key = sheet_cfg.get("replication_key")

            if sheet_name not in sheet_names:
                self.logger.warning(f"Sheet '{sheet_name}' not found in file. Skipping.")
                continue

            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)

                if df.empty or len(df.columns) == 0:
                    self.logger.info(f"Sheet '{sheet_name}' is empty. Skipping.")
                    continue

                if replication_key and replication_key not in df.columns:
                    self.logger.error(f"Replication key column '{replication_key}' is missing from sheet '{sheet_name}'. Failing sync.")
                    raise ValueError(f"Replication key column '{replication_key}' is missing from sheet '{sheet_name}'.")

                # Infer schema from DataFrame dtypes
                schema = {
                    "type": "object",
                    "properties": {}
                }
                for col in df.columns:
                    dtype = df[col].dtype
                    if pd.api.types.is_integer_dtype(dtype):
                        col_type = "integer"
                    elif pd.api.types.is_float_dtype(dtype):
                        col_type = "number"
                    elif pd.api.types.is_bool_dtype(dtype):
                        col_type = "boolean"
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        col_type = "string"  # or include "format": "date-time"
                    else:
                        col_type = "string"
                    schema["properties"][col] = {"type": [col_type, "null"]}

                records = df.to_dict(orient="records")

                streams.append(
                    ExcelStream(
                        tap=self,
                        name=sheet_name,
                        schema=schema,
                        records=records,
                        replication_key=replication_key
                    )
                )
            except Exception as e:
                self.logger.warning(f"Failed to load sheet '{sheet_name}': {e}")
                continue

        return streams


if __name__ == "__main__":
    TapExcel.cli()

cli = TapExcel.cli
