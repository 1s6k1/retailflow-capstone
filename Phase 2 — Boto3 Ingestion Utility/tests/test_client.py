import pytest
from unittest.mock import MagicMock, patch
from botocore.exceptions import EndpointConnectionError
from s3_ingest import S3ClientWrapper

@patch('s3_ingest.client.boto3.Session')
def test_upload_retry_on_failure(mock_session):
    """
    Tests that upload_file attempts retries when encountering EndpointConnectionError.
    """
    mock_client = MagicMock()
    
    # Simulate: Fail twice with network connection errors, succeed on the 3rd attempt
    mock_client.upload_file.side_effect = [
        EndpointConnectionError(endpoint_url="https://s3.amazonaws.com"),
        EndpointConnectionError(endpoint_url="https://s3.amazonaws.com"),
        None  # Success
    ]
    mock_session.return_value.client.return_value = mock_client
    
    wrapper = S3ClientWrapper()
    result = wrapper.upload_file("orders_day1.json", "retailflow-bucket-2026", "raw/orders/dt=2026-07-08/orders_day1.json")
    
    assert result is True
    assert mock_client.upload_file.call_count == 3