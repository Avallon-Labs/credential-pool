# credential-pool

Hands out credentials so that at most one node uses each of them at a time, using an S3 bucket as the only shared state.

## Usage

```python
import boto3
from credential_pool import CredentialPool, S3LeaseBackend

pool = CredentialPool(S3LeaseBackend(boto3.client("s3"), bucket="my-bucket", prefix="credential-leases/"))
credentials = {"res-a": ..., "res-b": ..., "res-c": ...}

lease = pool.claim(list(credentials), timeout=30)
if lease is None:
    raise RuntimeError("all credentials are in use")
try:
    while work_left():
        if not lease.held:
            raise RuntimeError("lost the credential")
        do_step(credentials[lease.id])
finally:
    pool.release(lease)
```

- `claim` picks a free credential from the ids you pass and returns a `Lease`, or `None` once `timeout` runs out. Credentials never need to exist in S3 beforehand, and their secrets never go to S3.
- The lease renews itself in a background thread until `release`.
- Check `lease.held` right before every use. It turns `False` when the lease was released, taken over, or could not be renewed before its deadline.

## Backends

`CredentialPool` (`credential_pool/pool.py`) handles waiting, renewal and deadlines for any backend. A backend (`credential_pool/backend.py`) only stores leases:

| Method | Must |
|---|---|
| `claim(ids, ttl)` | atomically take one id that is free or whose lease expired, return `(id, token)` or `None` |
| `renew(id, token, ttl)` | extend the lease only if `token` is still current, return the next token or `None` if it was taken over |
| `release(id, token)` | remove the lease only if `token` is still current |

A written lease must not expire within `ttl` seconds of the call starting. The pool starts the holder's deadline just before each call, so the holder always stops before the backend lets anyone else in. A Redis backend would be `SET NX PX` with a random token plus two compare-and-act Lua scripts; a Postgres backend would be one row per id with `expires_at = now() + ttl`, updated with `WHERE token = ...`. Raise on transient errors: the pool logs them and retries the renewal until the deadline.

## S3 bucket requirements

Any bucket works without setup. The caller needs `s3:ListBucket` on the bucket and `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on `<prefix>*`. On a versioned bucket every renewal leaves a noncurrent version behind, so either leave versioning off or expire noncurrent versions with a lifecycle rule.

## How the S3 backend works

- One object per credential at `<prefix><id>`. A missing object means the credential is free.
- **Claim:** one `ListObjectsV2` on the prefix, then a random pick among credentials that are missing or expired (`LastModified + ttl` at or before the `Date` header S3 sent with the listing). The claim is a `PutObject` with `If-None-Match: *` or `If-Match: <listed ETag>`. Losing a race moves on to the next free credential from the same listing.
- **Renew:** `PutObject` with `If-Match: <own ETag>`. Every write has a fresh random body, so the ETag changes on every renewal.
- **Release:** `DeleteObject` with `If-Match: <own ETag>`, so a lease that was already taken over is left alone.
- **Expiry:** S3 stamps `LastModified` after the call started and other nodes compare it against S3's own clock, which is what satisfies the backend contract without trusting any node's clock.

## Tuning

- `ttl` (default 60s): how long a crashed node keeps its credential blocked. Every node sharing a prefix must use the same `ttl`.
- `margin` (default 5s): must cover S3's one-second timestamp granularity plus the longest gap between checking `lease.held` and actually using the credential.
- `poll_interval` (default 1s): how often a waiting `claim` asks the backend again.
- The pool renews every `(ttl - margin) / 3` seconds, so a holder gets two renewal attempts before its deadline.

## Limits

- No fencing: a node that freezes between checking `lease.held` and using the credential can still use it after losing it. Keep that gap well below `margin`.
- No queue: waiting nodes poll, so a node can wait much longer than average when credentials are scarce.
- Deadlines use `time.monotonic()`, which doesn't count time the machine spends suspended.

## Tests

```sh
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
```

## Load test

```sh
.venv/bin/python loadtest.py --profile aws-dev --bucket <bucket>
```

Runs `--workers` processes on one machine that keep claiming, holding and releasing `--credentials` credentials for `--duration` seconds. It also injects failures: hard exits while holding a lease (`--crash-rate`), and `SIGSTOP` freezes of lease holders that last longer than the ttl (`--freeze-every`, `--freeze-for`). Each worker records when it actually used a credential. The run fails with exit code 1 if two workers ever used the same credential at overlapping times, or if a worker died unexpectedly. Workers share the machine's monotonic clock, which the overlap check relies on.

Every run writes to a fresh prefix. Leases abandoned by simulated crashes stay behind as 16-byte objects under it.
