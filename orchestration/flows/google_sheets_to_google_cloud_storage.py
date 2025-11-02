"""Flow to extract data from Google Sheets and load it into Google Cloud Storage."""

from prefect import flow

from config.extractors import GoogleSheetsExtractorConfig
from orchestration.tasks.extract import extract_from_google_sheets
from orchestration.tasks.load import load_to_google_cloud_storage


@flow(name="google_sheets_to_google_cloud_storage")
def google_sheets_to_google_cloud_storage(
    spreadsheet_id: str,
    sheet_name: str,
    bucket_name: str,
    blob_name: str,
    credentials_file_path: str,
) -> None:
    """Extract data from Google Sheets and load it into Google Cloud Storage."""
    data = extract_from_google_sheets(spreadsheet_id, sheet_name, credentials_file_path)
    load_to_google_cloud_storage(data, bucket_name, blob_name, credentials_file_path)


if __name__ == "__main__":
    google_sheets_config = GoogleSheetsExtractorConfig()

    for sheet_name in google_sheets_config.sheet_names:
        google_sheets_to_google_cloud_storage(
            credentials_file_path=google_sheets_config.credentials_file_path,
            spreadsheet_id=google_sheets_config.spreadsheet_id,
            bucket_name=google_sheets_config.bucket_name,
            sheet_name=sheet_name,
            blob_name=f"{sheet_name}/{sheet_name}",
        )
