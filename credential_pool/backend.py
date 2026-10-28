from abc import ABC, abstractmethod
from collections.abc import Sequence


class LeaseBackend(ABC):
    """Stores the leases of a CredentialPool.

    `claim` atomically takes one id that is free or whose lease expired and returns it with a token.
    `renew` returns the next token, or None once the lease was taken over. `renew` and `release` must
    only act while `token` is current, and a written lease must not expire within `ttl` seconds of the
    call starting.
    """

    @abstractmethod
    def claim(self, ids: Sequence[str], ttl: float) -> tuple[str, str] | None: ...

    @abstractmethod
    def renew(self, credential_id: str, token: str, ttl: float) -> str | None: ...

    @abstractmethod
    def release(self, credential_id: str, token: str) -> None: ...
