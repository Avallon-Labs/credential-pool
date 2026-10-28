import random
import uuid
from collections.abc import Sequence
from datetime import timedelta
from email.utils import parsedate_to_datetime

from botocore.exceptions import ClientError

from .backend import LeaseBackend


class S3LeaseBackend(LeaseBackend):
    def __init__(self, s3, bucket: str, prefix: str):
        self._s3 = s3
        self._bucket = bucket
        self._prefix = prefix

    def claim(self, ids: Sequence[str], ttl: float) -> tuple[str, str] | None:
        listing = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=self._prefix)
        now = parsedate_to_datetime(listing["ResponseMetadata"]["HTTPHeaders"]["date"])
        existing = {obj["Key"]: obj for obj in listing.get("Contents", [])}
        free = []
        for credential_id in ids:
            obj = existing.get(self._prefix + credential_id)
            if obj is None:
                free.append((credential_id, {"IfNoneMatch": "*"}))
            elif obj["LastModified"] + timedelta(seconds=ttl) <= now:
                free.append((credential_id, {"IfMatch": obj["ETag"]}))
        random.shuffle(free)
        for credential_id, condition in free:
            if etag := self._write(credential_id, condition):
                return credential_id, etag
        return None

    def renew(self, credential_id: str, token: str, ttl: float) -> str | None:
        return self._write(credential_id, {"IfMatch": token})

    def release(self, credential_id: str, token: str) -> None:
        try:
            self._s3.delete_object(Bucket=self._bucket, Key=self._prefix + credential_id, IfMatch=token)
        except ClientError as e:
            if _status(e) not in (404, 409, 412):
                raise

    def _write(self, credential_id: str, condition: dict) -> str | None:
        key = self._prefix + credential_id
        body = uuid.uuid4().bytes
        try:
            return self._s3.put_object(Bucket=self._bucket, Key=key, Body=body, **condition)["ETag"]
        except ClientError as e:
            if _status(e) not in (404, 409, 412):
                raise
            # botocore retries timeouts, so a write that landed can still come back as 412 on the retry.
            return self._etag_if_ours(key, body) if e.response["ResponseMetadata"]["RetryAttempts"] else None

    def _etag_if_ours(self, key: str, body: bytes) -> str | None:
        try:
            current = self._s3.get_object(Bucket=self._bucket, Key=key)
        except ClientError as e:
            if _status(e) == 404:
                return None
            raise
        return current["ETag"] if current["Body"].read() == body else None


def _status(error: ClientError) -> int:
    return error.response["ResponseMetadata"]["HTTPStatusCode"]
