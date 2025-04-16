from pathlib import Path
from typing import Dict, List, Optional, Iterable, Any
from datetime import datetime

import pandas as pd
from singer_sdk import Stream, Tap
from singer_sdk import typing as th


class ExcelStream(Stream):
    """Stream class for Excel sheets."""
    name = "excel-stream"

    def __init__(self, tap: Tap, sheet_name: str, file_path: Path, **kwargs):
        self.sheet_name = sheet_name
        self.file_path = file_path
        self._schema = None
        super().__init__(tap=tap, **kwargs)

    def _infer_column_type(self, series: pd.Series, sample_size: int = 100) -> th.Property:
        """Enhanced column type inference with metadata-style detection"""
        if series.isna().all():
            return th.StringType()

        sample = series.dropna().head(sample_size)

        if pd.api.types.is_datetime64_any_dtype(series):
            return th.DateTimeType()

        if series.dtype == 'bool' or sample.astype(str).str.lower().isin(['true', 'false']).all():
            return th.BooleanType()

        if pd.api.types.is_numeric_dtype(series):
            if (series.dropna() % 1 == 0).all():
                return th.IntegerType()
            return th.NumberType()

        str_sample = sample.astype(str)
        date_formats = [
            (r'\d{4}-\d{2}-\d{2}', 'date'),
            (r'\d{2}/\d{2}/\d{4}', 'date'),
            (r'\d{4}/\d{2}/\d{2}', 'date'),
            (r'\d{2}-\d{2}-\d{4}', 'date'),
            (r'\d{4}\.\d{2}\.\d{2}', 'date'),
            (r'\d{2}:\d{2}:\d{2}', 'time'),
            (r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', 'date-time')
        ]

        for pattern, fmt in date_formats:
            if str_sample.str.match(pattern).any():
                if fmt == 'date':
                    return th.DateType()
                elif fmt == 'time':
                    return th.TimeType()
                return th.DateTimeType()

        if str_sample.str.match(r'^[£$€¥]\s?\d+\.?\d*$').any():
            return th.StringType()

        return th.StringType()

    def _get_schema(self) -> th.PropertiesList:
        """Get stream schema."""
        properties = th.PropertiesList()

        try:
            if self.file_path.suffix == '.xls':
                engine = 'xlrd'
            elif self.file_path.suffix == '.ods':
                engine = 'odf'
            else:
                engine = None

            df = pd.read_excel(self.file_path, sheet_name=self.sheet_name, engine=engine)
        except Exception as e:
            self.logger.error(f"Error reading sheet '{self.sheet_name}': {str(e)}")
            return properties

        for col in df.columns:
            prop_type = self._infer_column_type(df[col])
            properties.append(th.Property(col, prop_type))

        return properties

    @property
    def schema(self) -> dict:
        """Get schema dictionary."""
        if self._schema is None:
            self._schema = self._get_schema().to_dict()
        return self._schema

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects."""
        try:
            if self.file_path.suffix == '.xls':
                engine = 'xlrd'
            elif self.file_path.suffix == '.ods':
                engine = 'odf'
            else:
                engine = None

            df = pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                engine=engine,
                parse_dates=True,
                keep_default_na=False
            )
        except Exception as e:
            self.logger.error(f"Error reading sheet '{self.sheet_name}': {str(e)}")
            return iter([])

        for _, row in df.iterrows():
            record = row.to_dict()
            for k, v in record.items():
                if pd.isna(v):
                    record[k] = None
                elif isinstance(v, pd.Timestamp):
                    record[k] = v.isoformat()
                elif isinstance(v, (pd.Timedelta, datetime)):
                    record[k] = str(v)
            yield record


class ExcelTap(Tap):
    """Excel tap class."""
    name = "excel-tap"

    def __init__(self, config=None, catalog=None, state=None, **kwargs):
        super().__init__(config=config, catalog=catalog, state=state, **kwargs)
        self.file_path = Path(self.config["file_path"])

        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")

    def _get_sheet_names(self) -> List[str]:
        """Get list of all available sheets in the Excel file."""
        try:
            if self.file_path.suffix == '.xls':
                engine = 'xlrd'
            elif self.file_path.suffix == '.ods':
                engine = 'odf'
            else:
                engine = None

            with pd.ExcelFile(self.file_path, engine=engine) as xls:
                return xls.sheet_names
        except Exception as e:
            self.logger.error(f"Error reading Excel file: {str(e)}")
            raise

    def discover_streams(self) -> List[Stream]:
        """Discover available streams (sheets)."""
        available_sheets = self._get_sheet_names()
        sheets_to_sync = self.config.get("sheets")

        if sheets_to_sync:
            sheets_to_process = [s for s in sheets_to_sync if s in available_sheets]
            missing_sheets = set(sheets_to_sync) - set(available_sheets)
            if missing_sheets:
                self.logger.warning(f"Requested sheets not found: {missing_sheets}")
        else:
            sheets_to_process = available_sheets

        return [
            ExcelStream(tap=self, sheet_name=sheet, file_path=self.file_path)
            for sheet in sheets_to_process
        ]


def cli():
    """CLI entry point."""
    ExcelTap.cli()


if __name__ == "__main__":
    cli()
