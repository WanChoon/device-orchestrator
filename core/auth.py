"""Credentials, tokens and cookies for a fleet that logs in hundreds of times.

Auth shows up in an orchestrator as three problems that have nothing to do with
each other, and conflating them is how automation ends up locked out of the
system it is automating.

**1. Secrets must not travel inside a TaskSpec.** A `TaskSpec` is serialised to
JSON, accepted over HTTP, echoed into `/tasks/{id}`, and written to the log on
every state change. A password in `spec.params` is therefore a password in the
log file, and no amount of scrubbing downstream fixes that -- the fix is that it
was never there. Specs carry a *reference* (`"credential": "demo-bank"`); the
store resolves it inside the target, at the moment of use.

**2. A token's expiry is knowable, so waiting for a 401 is a choice.** A JWT
carries `exp` in cleartext. Refreshing at `exp - skew` costs one request;
discovering expiry by failing a task costs the task, its retry, and a
device-blaming signal that was never the device's fault. This module reads the
claim. It does *not* verify the signature: the client is not the verifier, and
checking a signature against a key we also hold would be theatre.

**3. Re-authenticating is the expensive operation, not the cheap one.** Twenty
workers whose token expired at the same instant will, absent coordination, fire
twenty logins at the same second -- which to the far side is indistinguishable
from credential stuffing, and is how a fleet gets rate-limited or an account
gets locked. Refresh is single-flight per realm, and sessions are reused across
tasks so N tasks do not mean N logins.

Encryption at rest uses AES-GCM when `cryptography` is installed. It is not a
required dependency, so there is a stdlib fallback: scrypt for key derivation,
an HMAC-SHA256 counter-mode keystream, encrypt-then-MAC, constant-time tag
comparison. That construction is sound but it is hand-rolled, which is a thing
to do deliberately and once. It is here so the repo can be evaluated without
installing anything; the honest production answer is AES-GCM via `cryptography`
or, better, never holding the key at all and asking a KMS. `SecretBox` is the
one seam either answer plugs into.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from obs.log import get_logger

log = get_logger("core.auth")

try:  # pragma: no cover - exercised by whichever branch the host provides
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore

    AESGCM_AVAILABLE = True
except ImportError:  # pragma: no cover
    AESGCM = None  # type: ignore
    AESGCM_AVAILABLE = False


SCRYPT_N = 2 ** 14
SCRYPT_R = 8
SCRYPT_P = 1


class AuthError(RuntimeError):
    """Something is wrong with a credential, not with a device."""


class Secret:
    """A string that does not print itself.

    Most credential leaks are not exfiltration; they are a `repr()` in a stack
    trace, an f-string in a log line, or a dataclass that helpfully rendered all
    its fields. Making the leak require an explicit `.reveal()` turns an
    accident into a decision that shows up in review.
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return f"<Secret len={len(self._value)}>"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Secret):
            return NotImplemented
        return hmac.compare_digest(self._value, other._value)

    def __hash__(self) -> int:
        return hash(("Secret", len(self._value)))


@dataclass(frozen=True)
class Credential:
    """One way of proving identity to one system."""

    ref: str
    kind: str                                   # "basic" | "bearer" | "cookie"
    username: Optional[str] = None
    secret: Optional[Secret] = None
    realm: str = "default"
    extra: dict[str, Any] = field(default_factory=dict)

    def header(self) -> tuple[str, str]:
        """The Authorization header this credential produces, if any."""

        if self.kind == "basic":
            if self.username is None or self.secret is None:
                raise AuthError(f"credential {self.ref} is basic but incomplete")
            raw = f"{self.username}:{self.secret.reveal()}".encode()
            return "Authorization", "Basic " + base64.b64encode(raw).decode()
        if self.kind == "bearer":
            if self.secret is None:
                raise AuthError(f"credential {self.ref} is bearer but has no token")
            return "Authorization", "Bearer " + self.secret.reveal()
        raise AuthError(f"credential {self.ref} of kind {self.kind} has no header form")


# --------------------------------------------------------------------------
# Encryption at rest
# --------------------------------------------------------------------------


class SecretBox:
    """Authenticated encryption for credential material on disk.

    The output is versioned (`v1` stdlib, `v2` AES-GCM) because the format will
    change and a blob that cannot say what it is cannot be migrated. Each
    `seal()` draws a fresh random nonce: reusing a nonce under the same key
    breaks GCM catastrophically and leaks plaintext XOR under the stdlib
    keystream, so it is never derived from the message or a counter on disk.
    """

    def __init__(self, passphrase: str) -> None:
        if not passphrase:
            raise AuthError("SecretBox needs a passphrase")
        self._passphrase = passphrase.encode()

    def _derive(self, salt: bytes) -> bytes:
        # scrypt, not a bare hash: the threat is an offline attacker with the
        # file, and the only defence that matters there is making each guess
        # expensive in memory as well as time.
        return hashlib.scrypt(
            self._passphrase, salt=salt, n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P, dklen=64
        )

    def seal(self, plaintext: str) -> str:
        salt = secrets.token_bytes(16)
        nonce = secrets.token_bytes(12)
        key = self._derive(salt)
        data = plaintext.encode()

        if AESGCM_AVAILABLE:
            ciphertext = AESGCM(key[:32]).encrypt(nonce, data, None)  # type: ignore[misc]
            return _join("v2", salt, nonce, ciphertext, b"")

        enc_key, mac_key = key[:32], key[32:]
        ciphertext = _xor(data, _keystream(enc_key, nonce, len(data)))
        # Encrypt-then-MAC, over the nonce as well as the ciphertext: a MAC that
        # does not cover the nonce lets an attacker swap it and decrypt garbage
        # that still authenticates.
        tag = hmac.new(mac_key, nonce + ciphertext, hashlib.sha256).digest()
        return _join("v1", salt, nonce, ciphertext, tag)

    def open(self, blob: str) -> str:
        version, salt, nonce, ciphertext, tag = _split(blob)
        key = self._derive(salt)

        if version == "v2":
            if not AESGCM_AVAILABLE:
                raise AuthError("blob is AES-GCM but cryptography is not installed")
            try:
                return AESGCM(key[:32]).decrypt(nonce, ciphertext, None).decode()  # type: ignore[misc]
            except Exception as exc:  # noqa: BLE001 - InvalidTag and friends
                raise AuthError("credential blob failed authentication") from exc

        if version != "v1":
            raise AuthError(f"unknown secret format {version!r}")

        enc_key, mac_key = key[:32], key[32:]
        expected = hmac.new(mac_key, nonce + ciphertext, hashlib.sha256).digest()
        # Constant-time: a byte-at-a-time comparison here is a forgery oracle.
        if not hmac.compare_digest(expected, tag):
            raise AuthError("credential blob failed authentication")
        return _xor(ciphertext, _keystream(enc_key, nonce, len(ciphertext))).decode()


def _keystream(key: bytes, nonce: bytes, length: int) -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < length:
        out += hmac.new(key, nonce + counter.to_bytes(8, "big"), hashlib.sha256).digest()
        counter += 1
    return bytes(out[:length])


def _xor(data: bytes, pad: bytes) -> bytes:
    return bytes(a ^ b for a, b in zip(data, pad))


def _join(version: str, *parts: bytes) -> str:
    return "$".join([version] + [base64.urlsafe_b64encode(p).decode() for p in parts])


def _split(blob: str) -> tuple[str, bytes, bytes, bytes, bytes]:
    pieces = blob.split("$")
    if len(pieces) != 5:
        raise AuthError("malformed secret blob")
    version = pieces[0]
    salt, nonce, ciphertext, tag = (base64.urlsafe_b64decode(p) for p in pieces[1:])
    return version, salt, nonce, ciphertext, tag


# --------------------------------------------------------------------------
# JWT
# --------------------------------------------------------------------------


def jwt_claims(token: str) -> dict[str, Any]:
    """Read a JWT payload without verifying it.

    Deliberate: verification is the server's job and the server has the key. The
    client reads `exp` for one reason -- to stop using a token before it stops
    working -- and treating an unverified claim as a scheduling hint rather than
    as an authorisation decision is the whole distinction.
    """

    parts = token.split(".")
    if len(parts) != 3:
        raise AuthError("not a JWT")
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)          # base64url in JWTs is unpadded
    try:
        return json.loads(base64.urlsafe_b64decode(payload))
    except Exception as exc:  # noqa: BLE001
        raise AuthError("JWT payload is not JSON") from exc


def jwt_expiry(token: str) -> Optional[float]:
    exp = jwt_claims(token).get("exp")
    return float(exp) if isinstance(exp, (int, float)) else None


# --------------------------------------------------------------------------
# Token cache
# --------------------------------------------------------------------------


@dataclass
class CachedToken:
    token: Secret
    expires_at: Optional[float]

    def stale(self, skew_s: float) -> bool:
        if self.expires_at is None:
            return False
        return time.time() >= self.expires_at - skew_s


class TokenCache:
    """One live token per realm, refreshed before it expires, exactly once.

    The lock is per realm rather than global so an expired token for one system
    does not stall tasks running against another.
    """

    def __init__(self, skew_s: float = 60.0) -> None:
        self._skew_s = skew_s
        self._tokens: dict[str, CachedToken] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, realm: str) -> asyncio.Lock:
        lock = self._locks.get(realm)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[realm] = lock
        return lock

    async def get(
        self, realm: str, refresh: Callable[[], Awaitable[str]]
    ) -> Secret:
        cached = self._tokens.get(realm)
        if cached is not None and not cached.stale(self._skew_s):
            return cached.token

        async with self._lock_for(realm):
            # Re-check inside the lock: whoever held it before us has already
            # done the refresh, and the point of single-flight is that the other
            # nineteen workers take this branch and issue no request at all.
            cached = self._tokens.get(realm)
            if cached is not None and not cached.stale(self._skew_s):
                return cached.token

            raw = await refresh()
            try:
                expires_at = jwt_expiry(raw)
            except AuthError:
                expires_at = None       # opaque token: we cannot see its expiry
            self._tokens[realm] = CachedToken(Secret(raw), expires_at)
            log.info(
                "auth.token_refreshed",
                realm=realm,
                jwt=expires_at is not None,
                ttl_s=round(expires_at - time.time()) if expires_at else None,
            )
            return self._tokens[realm].token

    def invalidate(self, realm: str) -> None:
        """Call this on a 401, and only on a 401.

        An expired token and a revoked one look identical to a task; the
        difference is that a revoked one will not be fixed by waiting, so the
        cached copy has to go even though `exp` still says it is fine.
        """

        if self._tokens.pop(realm, None) is not None:
            log.warning("auth.token_invalidated", realm=realm)


# --------------------------------------------------------------------------
# Cookies
# --------------------------------------------------------------------------


class CookieJar:
    """Browser session state, kept per realm and shared across tasks.

    Reusing a logged-in session is not an optimisation. A hundred tasks that
    each log in produce a hundred authentication events from one address in a
    few minutes, which is what a credential-stuffing detector is built to catch
    -- the automation gets locked out for behaving exactly like an attack. One
    login, many tasks, is both faster and the only shape that survives contact
    with a real login system.
    """

    def __init__(self) -> None:
        self._state: dict[str, list[dict[str, Any]]] = {}

    def load(self, realm: str) -> list[dict[str, Any]]:
        return list(self._state.get(realm, ()))

    def save(self, realm: str, cookies: list[dict[str, Any]]) -> None:
        self._state[realm] = [dict(c) for c in cookies]
        log.info("auth.cookies_saved", realm=realm, count=len(cookies))

    def clear(self, realm: str) -> None:
        self._state.pop(realm, None)

    def live(self, realm: str, now: Optional[float] = None) -> bool:
        """Whether the stored session still has an unexpired session cookie.

        A jar that holds only expired cookies is worse than an empty one: it
        makes the next task look logged in until the first protected request,
        at which point the failure surfaces somewhere unrelated to its cause.
        """

        now = now if now is not None else time.time()
        cookies = self._state.get(realm)
        if not cookies:
            return False
        for cookie in cookies:
            expires = cookie.get("expires")
            if expires in (None, -1) or float(expires) > now:
                return True
        return False


# --------------------------------------------------------------------------
# Store
# --------------------------------------------------------------------------


class CredentialStore:
    """Resolves a reference in a TaskSpec to a credential, and nothing else.

    Environment first, then the sealed file. Environment wins because that is
    how a container, a CI job and a developer's shell all inject a secret
    without a file existing, and because a checked-in file that silently
    overrides the environment is a very slow bug.
    """

    ENV_PREFIX = "ORCH_CRED_"

    def __init__(self, box: Optional[SecretBox] = None) -> None:
        self._box = box
        self._file_creds: dict[str, Credential] = {}

    def load_file(self, path: str) -> int:
        """Read credentials from a sealed JSON file."""

        if self._box is None:
            raise AuthError("a sealed credential file needs a SecretBox")
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
        for ref, entry in raw.items():
            self._file_creds[ref] = Credential(
                ref=ref,
                kind=str(entry.get("kind", "basic")),
                username=entry.get("username"),
                secret=Secret(self._box.open(entry["sealed"])) if entry.get("sealed") else None,
                realm=str(entry.get("realm", ref)),
                extra=dict(entry.get("extra") or {}),
            )
        log.info("auth.credentials_loaded", source="file", count=len(self._file_creds))
        return len(self._file_creds)

    def put(self, credential: Credential) -> None:
        self._file_creds[credential.ref] = credential

    def resolve(self, ref: str) -> Credential:
        env_key = self.ENV_PREFIX + ref.upper().replace("-", "_")
        raw = os.environ.get(env_key)
        if raw:
            username, _, password = raw.partition(":")
            kind = "basic" if password else "bearer"
            return Credential(
                ref=ref,
                kind=kind,
                username=username if password else None,
                secret=Secret(password or username),
                realm=ref,
            )
        credential = self._file_creds.get(ref)
        if credential is None:
            # Naming the reference is safe and necessary; naming what was
            # searched for is how an operator finds the missing variable.
            raise AuthError(f"no credential {ref!r} (looked for ${env_key}, then the store)")
        return credential

    def refs(self) -> list[str]:
        return sorted(self._file_creds)
