"""Tests for APK rollout and credential handling.

Both of these are areas where a green test that only covers the happy path is
actively misleading: an installer that reports success on a failed install and a
token cache that logs in twenty times both *work* right up until they matter. So
almost everything here is a failure path.

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api.server import Orchestrator  # noqa: E402
from api.ws import EventHub  # noqa: E402
from core.auth import (  # noqa: E402
    AuthError,
    CookieJar,
    Credential,
    CredentialStore,
    Secret,
    SecretBox,
    TokenCache,
    jwt_expiry,
)
from core.device import AdbResult, Device, Lease, StaticDeviceSource  # noqa: E402
from core.health import HealthPolicy  # noqa: E402
from core.scheduler import SchedulerPolicy  # noqa: E402
from core.session import SessionManager  # noqa: E402
from core.task import (  # noqa: E402
    FatalError,
    RetryableError,
    TaskContext,
    TaskSpec,
    TaskState,
)
from obs.log import configure, get_logger  # noqa: E402
from targets.apk import (  # noqa: E402
    ApkTarget,
    FakeAdb,
    classify_install_output,
    sha256_of,
)
from targets.web import FakeBrowserSessionFactory, WebTarget, browser_slots  # noqa: E402

configure("CRITICAL")


def ctx_for(device_id: str, *, budget_s: float = 30.0) -> TaskContext:
    import time as _time

    return TaskContext(
        task_id="task-test",
        correlation_id="cid-test",
        attempt=1,
        log=get_logger("test"),
        deadline=_time.monotonic() + budget_s,
        device_id=device_id,
    )


def lease_for(device_id: str) -> Lease:
    return Lease(device=Device(id=device_id, transport="usb", model="test", tags=("android",)))


def apk_file(directory: str, name: str = "app.apk", size: int = 4096) -> str:
    path = Path(directory) / name
    path.write_bytes(b"PK\x03\x04" + b"x" * size)
    return str(path)


class InstallVerdictTests(unittest.TestCase):
    """`adb install` has, across versions, exited 0 while printing a failure."""

    def test_success_is_the_only_pass(self) -> None:
        ok = AdbResult(returncode=0, stdout="Success\n", stderr="")
        self.assertIsNone(classify_install_output(ok))

    def test_failure_code_beats_a_zero_exit(self) -> None:
        # The exact shape that makes a rollout report green while the build is
        # not on the phone.
        lying = AdbResult(
            returncode=0,
            stdout="Failure [INSTALL_FAILED_UPDATE_INCOMPATIBLE: signatures do not match]",
            stderr="",
        )
        self.assertEqual(
            classify_install_output(lying), "INSTALL_FAILED_UPDATE_INCOMPATIBLE"
        )

    def test_silence_is_not_success(self) -> None:
        mute = AdbResult(returncode=0, stdout="", stderr="")
        self.assertEqual(classify_install_output(mute), "INSTALL_NO_VERDICT")

    def test_timeout_is_reported_as_a_timeout(self) -> None:
        stuck = AdbResult(returncode=-1, stdout="", stderr="timeout", timed_out=True)
        self.assertEqual(classify_install_output(stuck), "INSTALL_TIMED_OUT")


class InstallBlameTests(unittest.IsolatedAsyncioTestCase):
    """The three-way split is the part worth testing: same failure, different fleet cost."""

    async def _install(self, stdout: str, *, device: str = "phone-1") -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = apk_file(tmp)
            target = ApkTarget(FakeAdb({"install": stdout}))
            spec = TaskSpec.from_dict(
                {"kind": "apk", "steps": [
                    {"op": "install", "path": path, "package": "com.example.app"}
                ]}
            )
            await target.execute(spec, lease_for(device), ctx_for(device))

    async def test_signature_mismatch_is_fatal_everywhere(self) -> None:
        # Retrying this across a 20-phone bench produces 20 identical errors.
        with self.assertRaises(FatalError):
            await self._install("Failure [INSTALL_FAILED_UPDATE_INCOMPATIBLE]")

    async def test_abi_mismatch_retries_without_blaming_the_phone(self) -> None:
        with self.assertRaises(RetryableError) as caught:
            await self._install("Failure [INSTALL_FAILED_NO_MATCHING_ABIS]")
        # The phone is healthy; it is simply the wrong phone for this build.
        self.assertFalse(caught.exception.blames_device)

    async def test_out_of_space_blames_the_phone(self) -> None:
        with self.assertRaises(RetryableError) as caught:
            await self._install("Failure [INSTALL_FAILED_INSUFFICIENT_STORAGE]")
        # It will still be out of space for the next task, so quarantine it.
        self.assertTrue(caught.exception.blames_device)

    async def test_missing_build_is_fatal_not_retried(self) -> None:
        target = ApkTarget(FakeAdb())
        spec = TaskSpec.from_dict(
            {"kind": "apk", "steps": [{"op": "install", "path": "/nope/missing.apk"}]}
        )
        with self.assertRaises(FatalError):
            await target.execute(spec, lease_for("phone-1"), ctx_for("phone-1"))


class VerifyTests(unittest.IsolatedAsyncioTestCase):
    async def _verify(self, dumpsys: str, **expect: object) -> dict:
        target = ApkTarget(FakeAdb({"shell dumpsys package": dumpsys}))
        step = {"op": "verify", "package": "com.example.app", **expect}
        spec = TaskSpec.from_dict({"kind": "apk", "steps": [step]})
        result = await target.execute(spec, lease_for("phone-1"), ctx_for("phone-1"))
        return result["captured"]

    async def test_reads_the_version_back(self) -> None:
        captured = await self._verify("versionCode=42\nversionName=1.4.2\n")
        self.assertEqual(captured["version_name"], "1.4.2")
        self.assertEqual(captured["version_code"], 42)

    async def test_success_then_absent_blames_the_phone(self) -> None:
        # The install said Success and dumpsys has never heard of the package.
        # No other device reproduces this, so it is evidence about this one.
        with self.assertRaises(RetryableError) as caught:
            await self._verify("Unable to find package: com.example.app")
        self.assertTrue(caught.exception.blames_device)

    async def test_wrong_version_is_fatal(self) -> None:
        # A stale build installed cleanly. Retrying installs the same stale
        # build; the rollout is wrong, not the bench.
        with self.assertRaises(FatalError):
            await self._verify(
                "versionCode=41\nversionName=1.4.1\n", expect_version_name="1.4.2"
            )


class ProvenanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_digest_matches_the_bytes_that_shipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = apk_file(tmp)
            expected = hashlib.sha256(Path(path).read_bytes()).hexdigest()
            self.assertEqual(await sha256_of(path), expected)

    async def test_install_records_the_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = apk_file(tmp)
            target = ApkTarget(FakeAdb())
            spec = TaskSpec.from_dict(
                {"kind": "apk", "steps": [
                    {"op": "install", "path": path, "package": "com.example.app"}
                ]}
            )
            out = await target.execute(spec, lease_for("phone-1"), ctx_for("phone-1"))
            self.assertEqual(len(out["captured"]["sha256"]), 64)


class RolloutTests(unittest.IsolatedAsyncioTestCase):
    """A rollout is N pinned tasks, so one dead phone costs exactly one phone."""

    async def test_one_offline_device_does_not_stop_the_rollout(self) -> None:
        phones = [
            Device(id=f"phone-{n}", transport="usb", model="test", tags=("android",))
            for n in (1, 2, 3)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = apk_file(tmp)
            orchestrator = Orchestrator(
                source=StaticDeviceSource(phones),
                targets={"apk": ApkTarget(FakeAdb(fail_for_device="phone-2"))},
                scheduler_policy=SchedulerPolicy(workers=3, retry_base_s=0.01),
                health_policy=HealthPolicy(
                    interval_s=30.0, failures_to_quarantine=1, recovery_attempts=1
                ),
                hub=EventHub(),
                probe=lambda device_id: device_id != "phone-2",
            )
            await orchestrator.start()
            try:
                records = [
                    orchestrator.scheduler.submit(
                        TaskSpec.from_dict(
                            {
                                "kind": "apk",
                                "steps": [{"op": "install", "path": path,
                                           "package": "com.example.app"}],
                                "selector": {"device_id": phone.id},
                                "max_attempts": 1,
                            }
                        )
                    )
                    for phone in phones
                ]
                await orchestrator.scheduler.drain(timeout=20.0)
                states = {
                    r.spec.selector.device_id: orchestrator.scheduler.get(r.spec.id).state
                    for r in records
                }
            finally:
                await orchestrator.stop()

        self.assertIs(states["phone-1"], TaskState.SUCCEEDED)
        self.assertIs(states["phone-3"], TaskState.SUCCEEDED)
        self.assertIsNot(states["phone-2"], TaskState.SUCCEEDED)


class SecretBoxTests(unittest.TestCase):
    def test_round_trip(self) -> None:
        box = SecretBox("correct horse battery staple")
        self.assertEqual(box.open(box.seal("hunter2")), "hunter2")

    def test_each_seal_uses_a_fresh_nonce(self) -> None:
        box = SecretBox("pass")
        first, second = box.seal("same plaintext"), box.seal("same plaintext")
        # Identical ciphertext for identical plaintext would mean a fixed nonce,
        # which leaks equality and, under a keystream, leaks the plaintext.
        self.assertNotEqual(first, second)

    def test_tampering_is_detected(self) -> None:
        box = SecretBox("pass")
        blob = box.seal("transfer 100")
        version, salt, nonce, ciphertext, tag = blob.split("$")
        flipped = base64.urlsafe_b64decode(ciphertext)
        flipped = bytes([flipped[0] ^ 0x01]) + flipped[1:]
        forged = "$".join(
            [version, salt, nonce, base64.urlsafe_b64encode(flipped).decode(), tag]
        )
        with self.assertRaises(AuthError):
            box.open(forged)

    def test_wrong_passphrase_is_rejected_not_garbled(self) -> None:
        blob = SecretBox("right").seal("secret")
        with self.assertRaises(AuthError):
            SecretBox("wrong").open(blob)


class SecretTests(unittest.TestCase):
    def test_it_does_not_print_itself(self) -> None:
        secret = Secret("s3kr3t-token-value")
        self.assertNotIn("s3kr3t", repr(secret))
        self.assertNotIn("s3kr3t", f"{secret}")
        self.assertNotIn("s3kr3t", json.dumps({"c": str(secret)}))
        self.assertEqual(secret.reveal(), "s3kr3t-token-value")


class JwtTests(unittest.TestCase):
    @staticmethod
    def token(**claims: object) -> str:
        def seg(payload: dict) -> str:
            return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")

        return f"{seg({'alg': 'HS256'})}.{seg(dict(claims))}.sig"

    def test_reads_exp_without_verifying(self) -> None:
        soon = time.time() + 300
        self.assertAlmostEqual(jwt_expiry(self.token(exp=int(soon))), int(soon))

    def test_opaque_token_is_not_a_jwt(self) -> None:
        with self.assertRaises(AuthError):
            jwt_expiry("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

    def test_jwt_without_exp_has_no_expiry(self) -> None:
        self.assertIsNone(jwt_expiry(self.token(sub="nobody")))


class TokenCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_twenty_workers_cause_one_login(self) -> None:
        calls = 0

        async def refresh() -> str:
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.05)       # a real login is not instant
            return "opaque-token"

        cache = TokenCache()
        tokens = await asyncio.gather(*(cache.get("bank", refresh) for _ in range(20)))

        # Twenty simultaneous logins from one address is what a stuffing
        # detector is built to catch. Single-flight is not an optimisation.
        self.assertEqual(calls, 1)
        self.assertEqual({t.reveal() for t in tokens}, {"opaque-token"})

    async def test_it_refreshes_before_expiry_not_after(self) -> None:
        issued = []

        def make(ttl: int) -> str:
            def seg(p: dict) -> str:
                return base64.urlsafe_b64encode(json.dumps(p).encode()).decode().rstrip("=")

            return f"{seg({'alg':'none'})}.{seg({'exp': int(time.time() + ttl)})}.s"

        async def refresh() -> str:
            token = make(30 if not issued else 3600)
            issued.append(token)
            return token

        cache = TokenCache(skew_s=60.0)
        first = await cache.get("bank", refresh)
        second = await cache.get("bank", refresh)

        # The first token is still valid for 30s, but expires inside the skew
        # window, so it is replaced now rather than mid-task.
        self.assertEqual(len(issued), 2)
        self.assertNotEqual(first.reveal(), second.reveal())

    async def test_invalidate_forces_a_new_login(self) -> None:
        calls = 0

        async def refresh() -> str:
            nonlocal calls
            calls += 1
            return f"token-{calls}"

        cache = TokenCache()
        await cache.get("bank", refresh)
        cache.invalidate("bank")            # what a 401 means: revoked, not expired
        await cache.get("bank", refresh)
        self.assertEqual(calls, 2)


class CredentialStoreTests(unittest.TestCase):
    def test_env_beats_the_file(self) -> None:
        import os

        store = CredentialStore()
        store.put(Credential(ref="bank", kind="basic", username="file", secret=Secret("f")))
        os.environ["ORCH_CRED_BANK"] = "env-user:env-pass"
        try:
            self.assertEqual(store.resolve("bank").username, "env-user")
        finally:
            del os.environ["ORCH_CRED_BANK"]

    def test_missing_credential_names_what_it_looked_for(self) -> None:
        with self.assertRaises(AuthError) as caught:
            CredentialStore().resolve("nope")
        self.assertIn("ORCH_CRED_NOPE", str(caught.exception))

    def test_basic_header_is_correctly_encoded(self) -> None:
        credential = Credential(
            ref="x", kind="basic", username="alice", secret=Secret("s3cret")
        )
        name, value = credential.header()
        self.assertEqual(name, "Authorization")
        self.assertEqual(
            base64.b64decode(value.split(" ", 1)[1]).decode(), "alice:s3cret"
        )


class CookieJarTests(unittest.TestCase):
    def test_all_expired_is_the_same_as_empty(self) -> None:
        jar = CookieJar()
        jar.save("bank", [{"name": "session", "value": "x", "expires": time.time() - 1}])
        # A jar of dead cookies is worse than none: the next task looks logged
        # in until the first protected request fails somewhere unrelated.
        self.assertFalse(jar.live("bank"))

    def test_a_session_cookie_counts_as_live(self) -> None:
        jar = CookieJar()
        jar.save("bank", [{"name": "session", "value": "x", "expires": -1}])
        self.assertTrue(jar.live("bank"))


class WebAuthTests(unittest.IsolatedAsyncioTestCase):
    def _target(self, store: CredentialStore | None = None) -> WebTarget:
        return WebTarget(
            SessionManager(),
            FakeBrowserSessionFactory(),
            credentials=store,
            tokens=TokenCache(),
            cookies=CookieJar(),
        )

    async def test_basic_auth_reaches_the_browser(self) -> None:
        store = CredentialStore()
        store.put(
            Credential(ref="bank", kind="basic", username="alice", secret=Secret("s3cret"))
        )
        target = self._target(store)
        spec = TaskSpec.from_dict(
            {"kind": "web", "steps": [
                {"op": "auth", "mode": "basic", "credential": "bank", "realm": "bank"}
            ]}
        )
        lease = Lease(device=browser_slots(1)[0])
        await target.execute(spec, lease, ctx_for(lease.device.id))

        handle = await target._sessions.ensure(lease.device, target._factory, ctx_for(lease.device.id))
        self.assertIn("Authorization", handle.driver.headers)
        self.assertTrue(handle.driver.headers["Authorization"].startswith("Basic "))

    async def test_a_second_task_reuses_the_captured_session(self) -> None:
        target = self._target(CredentialStore())
        lease = Lease(device=browser_slots(1)[0])

        login = TaskSpec.from_dict(
            {"kind": "web", "steps": [
                {"op": "auth", "mode": "cookie", "realm": "bank"},
                {"op": "goto", "url": "https://example.test/login"},
                {"op": "capture_session", "realm": "bank", "token_key": "access_token"},
            ]}
        )
        first = await target.execute(login, lease, ctx_for(lease.device.id))
        self.assertFalse(first["captured"]["auth"]["reused"])
        self.assertGreater(first["captured"]["session"]["cookies"], 0)

        after = TaskSpec.from_dict(
            {"kind": "web", "steps": [{"op": "auth", "mode": "cookie", "realm": "bank"}]}
        )
        second = await target.execute(after, lease, ctx_for(lease.device.id))
        # One login, many tasks. This is the assertion that keeps the fleet
        # from looking like a credential-stuffing run.
        self.assertTrue(second["captured"]["auth"]["reused"])

    async def test_a_missing_credential_is_fatal_not_retried(self) -> None:
        target = self._target(CredentialStore())
        spec = TaskSpec.from_dict(
            {"kind": "web", "steps": [
                {"op": "auth", "mode": "basic", "credential": "absent"}
            ]}
        )
        lease = Lease(device=browser_slots(1)[0])
        # Wrong credentials fail identically on every slot. Retrying them is a
        # faster way to get the account locked.
        with self.assertRaises(FatalError):
            await target.execute(spec, lease, ctx_for(lease.device.id))


class AndroidSecretTests(unittest.IsolatedAsyncioTestCase):
    """The same rule as the web target: the spec carries a reference, not a value."""

    async def test_the_password_never_appears_in_the_spec(self) -> None:
        from targets.android import AndroidTarget, FakeAppiumSessionFactory

        store = CredentialStore()
        store.put(
            Credential(ref="app", kind="basic", username="alice", secret=Secret("s3cret"))
        )
        spec = TaskSpec.from_dict(
            {"kind": "android", "steps": [
                {"op": "type_secret", "id": "pw", "credential": "app"}
            ]}
        )
        # The serialised spec is what crosses HTTP and lands in the log, so it
        # is the thing that has to be clean.
        self.assertNotIn("s3cret", json.dumps(spec.to_dict()))

        target = AndroidTarget(
            SessionManager(), FakeAppiumSessionFactory(), credentials=store
        )
        await target.execute(spec, lease_for("phone-1"), ctx_for("phone-1"))

    async def test_missing_store_is_fatal_not_retried(self) -> None:
        from targets.android import AndroidTarget, FakeAppiumSessionFactory

        target = AndroidTarget(SessionManager(), FakeAppiumSessionFactory())
        spec = TaskSpec.from_dict(
            {"kind": "android", "steps": [
                {"op": "type_secret", "id": "pw", "credential": "app"}
            ]}
        )
        with self.assertRaises(FatalError):
            await target.execute(spec, lease_for("phone-1"), ctx_for("phone-1"))


if __name__ == "__main__":
    unittest.main()
