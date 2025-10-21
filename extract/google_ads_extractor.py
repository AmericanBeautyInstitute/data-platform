"""Google Ads data Extractor."""

from typing import Any

import pyarrow as pa
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict

from extract.extractor import Extractor
from helpers.authenticator import google_ads_authenticator


class GoogleAdsExtractor(Extractor):
    """Extractor for Google Ads."""

    def extract(
        self,
        customer_id: str,
        query: str,
        login_customer_id: str | None = None,
    ) -> pa.Table:
        """Extracts data from Goolge Ads and returns results as a PyArrow table."""
        google_ads_client = self.client
        ga_service = google_ads_client.get_service("GoogleAdsService")

        response = ga_service.search(
            customer_id=customer_id,
            query=query,
            login_customer_id=login_customer_id,
        )

        table = self._convert_protobuf_to_table(response)
        return table

    def _authenticate(self) -> GoogleAdsClient:
        """Authenticates and returns Google Ads client."""
        google_ads_client = google_ads_authenticator(self.credentials_file_path)
        return google_ads_client

    def _convert_protobuf_to_table(self, response: Any) -> pa.Table:
        """Converts Google Ads API response to PyArrow table."""
        flattened_rows = []
        for protobuf_row in response:
            nested_dict = MessageToDict(protobuf_row._pb)
            flattened_dict = self._flatten_nested_dict(nested_dict)
            flattened_rows.append(flattened_dict)

        table = self._build_table_from_dicts(flattened_rows)
        return table

    def _build_columns(
        self,
        rows: list[dict[str, Any]],
        column_names: list[str],
    ) -> dict[str, list[Any]]:
        """Builds column data from rows, filling missing values with None."""
        columns = {}
        for column_name in column_names:
            column_data = [row.get(column_name) for row in rows]
            columns[column_name] = column_data
        return columns

    def _build_key(self, prefix: str, field_name: str) -> str:
        """Builds a dot-notation key from prefix and field name."""
        key = f"{prefix}.{field_name}" if prefix else field_name
        return key

    def _build_table_from_dicts(self, rows: list[dict[str, Any]]) -> pa.Table:
        """Builds a PyArrow table from a list of dictionaries."""
        if not rows:
            empty_table = pa.table({})
            return empty_table

        all_column_names = self._get_column_names(rows)
        columns = self._build_columns(rows, all_column_names)
        table = pa.table(columns)
        return table

    def _flatten_nested_dict(
        self,
        data: dict[str, Any],
        prefix: str = "",
    ) -> dict[str, Any]:
        """Flattens a nested dictionary using dot notation.

        Example:
            Input:  {"segments": {"date": "2024-01-01"}}
            Output: {"segments.date": "2024-01-01"}
        """
        flattened = {}

        for field_name, field_value in data.items():
            full_key = self._build_key(prefix, field_name)

            if self._is_nested_dict(field_value):
                nested_flattened = self._flatten_nested_dict(field_value, full_key)
                flattened.update(nested_flattened)
            else:
                flattened[full_key] = field_value

        return flattened

    def _get_column_names(self, rows: list[dict[str, Any]]) -> list[str]:
        """Gets all unique column names across rows, handling sparse data."""
        column_names: set[str] = set()
        for row in rows:
            column_names.update(row.keys())

        sorted_column_names = sorted(column_names)
        return sorted_column_names

    def _is_nested_dict(self, value: Any) -> bool:
        """Checks if a value is a nested dictionary."""
        is_dict = isinstance(value, dict)
        return is_dict
