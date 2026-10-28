import logging
import random
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass, field

from .backend import LeaseBackend

log = logging.getLogger(__name__)


@dataclass(eq=False)
class Lease:
    id: str
    token: str
    deadline: float
    stopped: threading.Event = field(default_factory=threading.Event)
    heartbeat: threading.Thread | None = None

    @property
    def held(self) -> bool:
        return not self.stopped.is_set() and time.monotonic() < self.deadline


class CredentialPool:
    """Hands out at most one lease per credential id at a time.

    Leases renew themselves until released. `held` turns False as soon as the lease is lost
    or could not be renewed in time, so check it right before every use of the credential.
    """

    def __init__(self, backend: LeaseBackend, ttl: float = 60, margin: float = 5, poll_interval: float = 1):
        self._backend = backend
        self._ttl = ttl
        self._margin = margin
        self._poll_interval = poll_interval

    def claim(self, ids: Sequence[str], timeout: float = 0) -> Lease | None:
        give_up = time.monotonic() + timeout
        while (lease := self._try_claim(ids)) is None:
            remaining = give_up - time.monotonic()
            if remaining <= 0:
                return None
            time.sleep(min(remaining, random.uniform(0.5, 1.5) * self._poll_interval))
        lease.heartbeat = threading.Thread(target=self._keep_alive, args=(lease,), daemon=True)
        lease.heartbeat.start()
        return lease

    def release(self, lease: Lease) -> None:
        lease.stopped.set()
        lease.heartbeat.join()
        self._backend.release(lease.id, lease.token)

    def _try_claim(self, ids: Sequence[str]) -> Lease | None:
        started = time.monotonic()
        if granted := self._backend.claim(ids, self._ttl):
            return Lease(*granted, deadline=started + self._ttl - self._margin)
        return None

    def _keep_alive(self, lease: Lease) -> None:
        while not lease.stopped.wait((self._ttl - self._margin) / 3):
            started = time.monotonic()
            try:
                token = self._backend.renew(lease.id, lease.token, self._ttl)
            except Exception:
                log.warning("renewing lease on %s failed", lease.id, exc_info=True)
                continue
            if token is None:
                log.warning("lost lease on %s", lease.id)
                lease.stopped.set()
            else:
                lease.token, lease.deadline = token, started + self._ttl - self._margin
