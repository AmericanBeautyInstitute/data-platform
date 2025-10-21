"""Google Analytics Data Extractor."""

from typing import Any

import pyarrow as pa
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    RunReportResponse,
)

from extract.extractor import Extractor
from helpers.google_cloud_platform import authenticate_google_analytics


class GoogleAnalyticsExtractor(Extractor):
    """Extractor for Google Analytics Data API (GA4)."""

    def extract(
        self,
        property_id: str,
        start_date: str,
        end_date: str,
        metrics: list[str],
        dimensions: list[str] | None = None,
    ) -> pa.Table:
        """Extracts data from Google Analytics and returns it as a PyArrow table."""
        client = self.client
        dimensions = dimensions or []

        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            metrics=[Metric(name=metric) for metric in metrics],
            dimensions=[Dimension(name=dimension) for dimension in dimensions],
        )

        response = client.run_report(request)
        table = self._convert_response_to_table(response, dimensions, metrics)
        return table

    def _authenticate(self) -> BetaAnalyticsDataClient:
        """Authenticates and returns Google Analytics Data API client."""
        client = authenticate_google_analytics(self.credentials_file_path)
        return client

    def _convert_response_to_table(
        self,
        response: RunReportResponse,
        dimension_names: list[str],
        metric_names: list[str],
    ) -> pa.Table:
        """Converts the Google Analytics API response to PyArrow table."""
        rows = []

        for row in response.rows:
            row_dict = {}

            # Extract dimension values
            for i, dimension_name in enumerate(dimension_names):
                row_dict[dimension_name] = row.dimension_values[i].value

            # Extract metric values
            for i, metric_name in enumerate(metric_names):
                row_dict[metric_name] = row.metric_values[i].value

            rows.append(row_dict)

        table = self._build_table_from_dicts(rows)
        return table

    def _build_table_from_dicts(self, rows: list[dict[str, Any]]) -> pa.Table:
        """Builds a PyArrow table from a list of dictionaries."""
        if not rows:
            empty_table = pa.table({})
            return empty_table

        all_column_names = self._collect_all_keys(rows)
        columns = self._build_columns(rows, all_column_names)
        table = pa.table(columns)
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

    def _collect_all_keys(self, rows: list[dict[str, Any]]) -> list[str]:
        """Collects all unique keys from rows, handling sparse data."""
        all_keys = set()
        for row in rows:
            all_keys.update(row.keys())

        sorted_keys = sorted(all_keys)
        return sorted_keys
