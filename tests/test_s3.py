import threading
import time

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from credential_pool import CredentialPool, S3LeaseBackend

BUCKET = "credential-pool-test"
IDS = ["res-a", "res-b", "res-c"]


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def make_pool(s3, **kwargs):
    return CredentialPool(S3LeaseBackend(s3, BUCKET, "leases/"), **kwargs)


def test_hands_out_each_credential_once(s3):
    pool = make_pool(s3)
    leases = [pool.claim(IDS) for _ in IDS]
    assert sorted(lease.id for lease in leases) == IDS
    assert pool.claim(IDS) is None
    for lease in leases:
        pool.release(lease)


def test_release_makes_credential_claimable_again(s3):
    pool = make_pool(s3)
    lease = pool.claim(["res-a"])
    pool.release(lease)
    assert not lease.held
    again = pool.claim(["res-a"])
    assert again.id == "res-a"
    pool.release(again)


def test_claim_waits_for_release(s3):
    pool = make_pool(s3, poll_interval=0.1)
    lease = pool.claim(["res-a"])
    threading.Timer(0.5, pool.release, [lease]).start()
    waited = pool.claim(["res-a"], timeout=5)
    assert waited.id == "res-a"
    pool.release(waited)


def test_heartbeat_keeps_lease_past_ttl(s3):
    pool = make_pool(s3, ttl=2, margin=0.5)
    lease = pool.claim(["res-a"])
    time.sleep(4)
    assert lease.held
    assert pool.claim(["res-a"]) is None
    pool.release(lease)


def test_abandoned_lease_is_taken_over_after_ttl(s3):
    pool = make_pool(s3, ttl=2, margin=0.5, poll_interval=0.2)
    crashed = pool.claim(["res-a"])
    crashed.stopped.set()
    assert pool.claim(["res-a"]) is None
    successor = pool.claim(["res-a"], timeout=5)
    assert successor.id == "res-a"
    pool.release(successor)


def test_holder_notices_takeover_and_release_keeps_new_lease(s3):
    pool = make_pool(s3, ttl=1.5, margin=0.3)
    lease = pool.claim(["res-a"])
    s3.put_object(Bucket=BUCKET, Key="leases/res-a", Body=b"someone else")
    time.sleep(1)
    assert not lease.held
    pool.release(lease)
    assert s3.get_object(Bucket=BUCKET, Key="leases/res-a")["Body"].read() == b"someone else"


def test_claim_survives_retried_write_that_already_landed(s3, monkeypatch):
    put = s3.put_object

    def put_then_report_conflict(**kwargs):
        put(**kwargs)
        raise ClientError(
            {"Error": {"Code": "PreconditionFailed"}, "ResponseMetadata": {"HTTPStatusCode": 412, "RetryAttempts": 1}},
            "PutObject",
        )

    monkeypatch.setattr(s3, "put_object", put_then_report_conflict)
    pool = make_pool(s3)
    lease = pool.claim(["res-a"])
    assert lease.held
    pool.release(lease)


def test_claim_moves_on_when_expired_lease_is_released_mid_takeover(s3, monkeypatch):
    pool = make_pool(s3, ttl=1, margin=0.5)
    pool.claim(["res-a"]).stopped.set()
    time.sleep(2.5)
    list_objects = s3.list_objects_v2

    def list_then_release(**kwargs):
        listing = list_objects(**kwargs)
        s3.delete_object(Bucket=BUCKET, Key="leases/res-a")
        return listing

    monkeypatch.setattr(s3, "list_objects_v2", list_then_release)
    assert pool.claim(["res-a"]) is None


def test_holder_notices_its_lease_was_deleted(s3):
    pool = make_pool(s3, ttl=1.5, margin=0.3)
    lease = pool.claim(["res-a"])
    s3.delete_object(Bucket=BUCKET, Key="leases/res-a")
    time.sleep(1)
    assert lease.stopped.is_set()
    pool.release(lease)
