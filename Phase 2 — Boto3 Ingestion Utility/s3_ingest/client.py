import os
import logging
from typing import List, Optional
import boto3
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

load_dotenv()

# Setup Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("s3_ingest")

class S3ClientWrapper:
    def __init__(self, profile_name: Optional[str] = None):
        profile = profile_name or os.getenv("AWS_PROFILE")
        if profile:
            self.session = boto3.Session(profile_name=profile)
        else:
            self.session = boto3.Session()
        self.client = self.session.client("s3")

    # Exponential Backoff Retry Rules
    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def upload_file(self, file_path: str, bucket: str, object_name: str) -> bool:
        logger.info(f"Uploading {file_path} to s3://{bucket}/{object_name}")
        self.client.upload_file(file_path, bucket, object_name)
        return True

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        logger.info(f"Listing s3://{bucket}/{prefix}")
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [item['Key'] for item in response.get('Contents', [])]

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def download_file(self, bucket: str, object_name: str, file_path: str) -> bool:
        logger.info(f"Downloading s3://{bucket}/{object_name} to {file_path}")
        self.client.download_file(bucket, object_name, file_path)
        return True

    @retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def generate_presigned_url(self, bucket: str, object_name: str, expiration: int = 3600) -> str:
        logger.info(f"Generating presigned URL for s3://{bucket}/{object_name}")
        return self.client.generate_presigned_url(
            'get_object', Params={'Bucket': bucket, 'Key': object_name}, ExpiresIn=expiration
        )