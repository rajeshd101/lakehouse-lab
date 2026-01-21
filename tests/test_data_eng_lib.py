import pytest
from unittest.mock import MagicMock, patch
from include.data_eng_lib import upload_to_cloud

def test_upload_to_cloud_unsupported():
    with pytest.raises(ValueError, match="Unsupported cloud: unknown"):
        upload_to_cloud("local.txt", "unknown", "bucket", "remote.txt")

@patch("include.data_eng_lib.upload_to_s3")
def test_upload_to_cloud_s3(mock_upload):
    mock_upload.return_value = "s3://bucket/remote.txt"
    result = upload_to_cloud("local.txt", "s3", "bucket", "remote.txt")
    assert result == "s3://bucket/remote.txt"
    mock_upload.assert_called_once_with("local.txt", "bucket", "remote.txt", None)

@patch("include.data_eng_lib.upload_to_gcs")
def test_upload_to_cloud_gcs(mock_upload):
    mock_upload.return_value = "gs://bucket/remote.txt"
    result = upload_to_cloud("local.txt", "gs", "bucket", "remote.txt")
    assert result == "gs://bucket/remote.txt"
    mock_upload.assert_called_once_with("local.txt", "bucket", "remote.txt", None)

@patch("include.data_eng_lib.upload_to_azure")
def test_upload_to_cloud_azure(mock_upload):
    mock_upload.return_value = "az://url/bucket/remote.txt"
    result = upload_to_cloud("local.txt", "az", "bucket", "remote.txt")
    assert result == "az://url/bucket/remote.txt"
    mock_upload.assert_called_once_with("local.txt", "bucket", "remote.txt", None)
