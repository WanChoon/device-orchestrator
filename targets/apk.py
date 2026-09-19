"""APK deployment as a task, not as a special case.

Pushing a build onto a fleet is the same shape as running a test on it: it needs
a device to itself, it takes minutes, it fails in device-specific ways, and it
must not be retried blindly. So deployment is a `Target` like any other and
inherits leases, deadlines, retries and blame from the scheduler without the
scheduler learning anything about packages.

It is also the target that proves the contract is actually narrow: `ApkTarget`
holds no `SessionManager`. Nothing in `Target.execute` requires a session -- that
was an Appium and Playwright detail, not a contract detail, and a target that
needs no remote session is the test of whether that was true.

Three things about `adb install` drive the rest of this module.

**The exit code is not the verdict.** `adb install`, and `adb shell pm install`
far more so, have across versions printed `Failure [INSTALL_FAILED_...]` on
stdout while exiting 0. Anything trusting `returncode` alone eventually reports
a green deploy of a build that is not on the phone, which is worse than a failed
deploy because nobody goes looking. The output is parsed, and the absence of an
explicit `Success` is itself a failure.

**Install failures split three ways, and the split is not the obvious one.**
A signature mismatch fails identically on every phone in the fleet, so retrying
it burns the whole bench to reproduce the same error N times: `FatalError`. A
device that dropped mid-transfer is the opposite -- the APK is fine, the phone is
not: `RetryableError(blames_device=True)`. And between them sits a third case
that neither bucket fits: an ABI or minSdk mismatch means this *healthy* phone is
the wrong phone. Retry it elsewhere, blame nobody.

**Installing is not the same as having installed.** A `Success` from the package
manager means the install session was committed, not that the version you wanted
is what a user would now launch. The two are different whenever a deploy races
another install, an OEM updater, or a work profile. So `verify` reads the
version back out of `dumpsys` as a separate step you can put in the task.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
from typing import Any, Optional

from core.device import AdbNotAvailable, AdbResult, Lease
from core.task import FatalError, RetryableError, Target, TaskContext, TaskSpec
from obs.log import get_logger

log = get_logger("targets.apk")


# Failures that will reproduce on every device in the fleet. The build is wrong,
# not the bench, and a retry is just a slower way to get the same message.
FATAL_INSTALL_CODES = frozenset(
    {
        "INSTALL_FAILED_UPDATE_INCOMPATIBLE",       # signed with a different key
        "INSTALL_FAILED_VERSION_DOWNGRADE",
        "INSTALL_FAILED_INVALID_APK",
        "INSTALL_FAILED_DUPLICATE_PACKAGE",
        "INSTALL_FAILED_CONFLICTING_PROVIDER",
        "INSTALL_PARSE_FAILED_NO_CERTIFICATES",
        "INSTALL_PARSE_FAILED_MANIFEST_MALFORMED",
        "INSTALL_PARSE_FAILED_NOT_APK",
        "INSTALL_FAILED_TEST_ONLY",                 # needs -t, and that is a caller decision
    }
)

# The device is unwell: it cannot hold the build right now. Worth quarantining,
# because a phone that is out of space stays out of space for the next task too.
DEVICE_INSTALL_CODES = frozenset(
    {
        "INSTALL_FAILED_INSUFFICIENT_STORAGE",
        "INSTALL_FAILED_MEDIA_UNAVAILABLE",
        "INSTALL_FAILED_INTERNAL_ERROR",
        "INSTALL_FAILED_PACKAGE_CHANGED",
    }
)

# The device is healthy and simply is not a candidate for this build. Sending the
# task somewhere else is right; holding the phone against it is not. The real fix
# is a selector -- `{"selector": {"tags": ["arm64"]}}` -- and this is the fallback
# for when the bench inventory has drifted from what the tags claim.
MISMATCH_INSTALL_CODES = frozenset(
    {
        "INSTALL_FAILED_NO_MATCHING_ABIS",
        "INSTALL_FAILED_OLDER_SDK",
        "INSTALL_FAILED_MISSING_SHARED_LIBRARY",
        "INSTALL_FAILED_CPU_ABI_INCOMPATIBLE",
    }
)

_FAILURE_RE = re.compile(r"Failure\s*\[([A-Z_0-9]+)")
_VERSION_NAME_RE = re.compile(r"versionName=(\S+)")
_VERSION_CODE_RE = re.compile(r"versionCode=(\d+)")

# Hashing a 60MB APK is blocking work on a thread, and N workers deploying at
# once would otherwise have N threads contending for the same disk. The pool is
# bounded so a wide fleet does not turn a deploy into an IO storm.
_HASH_SLOTS = asyncio.Semaphore(2)


class ApkNotFound(FatalError):
    """The build is missing locally. No device can fix that."""


async def sha256_of(path: str) -> str:
    """Provenance for the deploy record: which bytes actually went to the phone.

    A deploy log that names a file path records an intention; a deploy log that
    names a digest records an event. Paths get overwritten by the next CI run.
    """

    async with _HASH_SLOTS:
        return await asyncio.to_thread(_sha256_blocking, path)


def _sha256_blocking(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classify_install_output(result: AdbResult) -> Optional[str]:
    """Return the failure code, or None if the install genuinely succeeded.

    `Success` on stdout is the only positive signal that means anything. An
    install that neither says `Success` nor names a failure code has been
    truncated or the device vanished mid-sentence, and is reported as such
    rather than optimistically passed.
    """

    blob = f"{result.stdout}\n{result.stderr}"
    match = _FAILURE_RE.search(blob)
    if match:
        return match.group(1)
    if "Success" in result.stdout:
        return None
    if result.timed_out:
        return "INSTALL_TIMED_OUT"
    if not result.ok:
        return "INSTALL_FAILED_UNKNOWN"
    return "INSTALL_NO_VERDICT"


def raise_for_install_code(code: str, *, package: str, device_id: str) -> None:
    if code in FATAL_INSTALL_CODES:
        raise FatalError(f"{code} installing {package}: the build is wrong, not the bench")
    if code in MISMATCH_INSTALL_CODES:
        # Healthy phone, wrong phone. Retry elsewhere; do not hold it against it.
        raise RetryableError(
            f"{code}: {device_id} is not a candidate for {package}",
            blames_device=False,
        )
    if code in DEVICE_INSTALL_CODES or code in {"INSTALL_TIMED_OUT", "INSTALL_FAILED_UNKNOWN"}:
        raise RetryableError(f"{code} installing {package} on {device_id}", blames_device=True)
    # An unrecognised code is still a real failure. Treat it as device-blaming
    # only when adb itself could not complete, otherwise retry somewhere else.
    raise RetryableError(f"{code} installing {package}", blames_device=False)


class ApkTarget(Target):
    """Deploys and verifies builds over adb.

    Depends on an object with `Adb`'s `run` signature and nothing else, so the
    tests drive it with a scripted fake and the demo runs without a phone.
    """

    kind = "apk"

    def __init__(self, adb: Any, *, install_timeout_s: float = 180.0) -> None:
        self._adb = adb
        self._install_timeout_s = install_timeout_s

    async def execute(
        self, spec: TaskSpec, lease: Lease, ctx: TaskContext
    ) -> dict[str, Any]:
        device_id = lease.device.id
        captured: dict[str, Any] = {}

        for index, step in enumerate(spec.steps, start=1):
            if ctx.remaining_s() <= 0:
                raise RetryableError("ran out of time mid-deploy", blames_device=False)
            op = step.get("op")
            await ctx.progress("step", f"{index}/{len(spec.steps)} {op}", step=index, op=op)
            captured.update(await self._run_step(device_id, step, op, index, ctx))

        return {"steps": len(spec.steps), "device": device_id, "captured": captured}

    async def _run_step(
        self,
        device_id: str,
        step: dict[str, Any],
        op: Optional[str],
        index: int,
        ctx: TaskContext,
    ) -> dict[str, Any]:
        try:
            if op == "install":
                return await self._install(device_id, step, ctx)
            if op == "verify":
                return await self._verify(device_id, step, ctx)
            if op == "uninstall":
                return await self._uninstall(device_id, step, ctx)
            if op == "launch":
                return await self._launch(device_id, step, ctx)
            if op == "stop":
                await self._shell(device_id, ctx, "am", "force-stop", _package(step))
                return {}
            if op == "clear":
                await self._shell(device_id, ctx, "pm", "clear", _package(step))
                return {}
        except AdbNotAvailable as exc:
            # The host has no adb. Every device on this host will fail the same
            # way, so this is not the phone's fault and must not quarantine it.
            raise RetryableError(str(exc), blames_device=False) from exc

        raise FatalError(f"unknown apk op: {op!r}")

    async def _install(
        self, device_id: str, step: dict[str, Any], ctx: TaskContext
    ) -> dict[str, Any]:
        path = str(step.get("path") or "")
        if not path:
            raise FatalError("install step needs a path")
        if not os.path.isfile(path):
            raise ApkNotFound(f"no APK at {path}")

        package = str(step.get("package") or os.path.basename(path))
        digest = await sha256_of(path)
        await ctx.progress("apk", "hashed build", package=package, sha256=digest[:12])

        flags: list[str] = []
        if step.get("reinstall", True):
            flags.append("-r")          # keep data; also what makes a retry idempotent
        if step.get("grant_permissions", False):
            flags.append("-g")
        if step.get("allow_downgrade", False):
            flags.append("-d")
        if step.get("allow_test", False):
            flags.append("-t")

        # An install is the longest thing this system does to a phone, but it
        # still may not outlive the task that asked for it.
        budget = min(float(step.get("timeout_s", self._install_timeout_s)), ctx.remaining_s())
        result = await self._adb.run("install", *flags, path, serial=device_id, timeout=budget)

        code = classify_install_output(result)
        if code is not None:
            log.warning(
                "apk.install_failed",
                device=device_id,
                package=package,
                code=code,
                sha256=digest[:12],
            )
            raise_for_install_code(code, package=package, device_id=device_id)

        log.info("apk.installed", device=device_id, package=package, sha256=digest[:12])
        return {"installed": package, "sha256": digest, "bytes": os.path.getsize(path)}

    async def _verify(
        self, device_id: str, step: dict[str, Any], ctx: TaskContext
    ) -> dict[str, Any]:
        package = _package(step)
        result = await self._shell(
            device_id, ctx, "dumpsys", "package", package, timeout_s=step.get("timeout_s", 20.0)
        )
        name_match = _VERSION_NAME_RE.search(result.stdout)
        code_match = _VERSION_CODE_RE.search(result.stdout)
        if not name_match and not code_match:
            # dumpsys answered but knows nothing about the package: the install
            # reported Success and the phone disagrees. That is the bug this
            # step exists to catch, and no other device will reproduce it.
            raise RetryableError(
                f"{package} is not installed on {device_id} after a successful install",
                blames_device=True,
            )

        found_name = name_match.group(1) if name_match else None
        found_code = int(code_match.group(1)) if code_match else None

        expected_name = step.get("expect_version_name")
        expected_code = step.get("expect_version_code")
        if expected_name is not None and found_name != str(expected_name):
            raise FatalError(
                f"{package} is {found_name} on {device_id}, expected {expected_name}"
            )
        if expected_code is not None and found_code != int(expected_code):
            raise FatalError(
                f"{package} is versionCode {found_code} on {device_id}, expected {expected_code}"
            )

        return {"package": package, "version_name": found_name, "version_code": found_code}

    async def _uninstall(
        self, device_id: str, step: dict[str, Any], ctx: TaskContext
    ) -> dict[str, Any]:
        package = _package(step)
        args = ["uninstall"]
        if step.get("keep_data", False):
            args.append("-k")
        result = await self._adb.run(
            *args, package, serial=device_id, timeout=min(60.0, ctx.remaining_s())
        )
        missing = "not installed" in f"{result.stdout}{result.stderr}".lower()
        if not result.ok and not (missing and step.get("ignore_missing", True)):
            raise RetryableError(
                f"uninstall {package} failed on {device_id}", blames_device=True
            )
        # Uninstalling something that is already gone is the desired end state,
        # which is what makes this step safe under at-least-once retry.
        return {"uninstalled": package, "was_present": not missing}

    async def _launch(
        self, device_id: str, step: dict[str, Any], ctx: TaskContext
    ) -> dict[str, Any]:
        package = _package(step)
        activity = step.get("activity")
        if activity:
            await self._shell(device_id, ctx, "am", "start", "-n", f"{package}/{activity}")
        else:
            # No activity named: let the package manager resolve the launcher
            # entry rather than guessing `.MainActivity`, which is wrong often.
            await self._shell(
                device_id, ctx, "monkey", "-p", package,
                "-c", "android.intent.category.LAUNCHER", "1",
            )
        return {"launched": package}

    async def _shell(
        self,
        device_id: str,
        ctx: TaskContext,
        *args: str,
        timeout_s: float = 30.0,
    ) -> AdbResult:
        budget = min(float(timeout_s), max(ctx.remaining_s(), 1.0))
        result = await self._adb.run("shell", *args, serial=device_id, timeout=budget)
        if result.timed_out:
            raise RetryableError(
                f"adb shell {args[0]} timed out on {device_id}", blames_device=True
            )
        return result


def _package(step: dict[str, Any]) -> str:
    package = step.get("package")
    if not package:
        raise FatalError(f"step {step.get('op')!r} needs a package")
    return str(package)


class FakeAdb:
    """Scripted adb for the demo and the tests.

    `responses` maps a command prefix to the text adb would print. The default
    behaviour is a successful install of an app that then verifies, so a test
    only has to describe the failure it cares about.
    """

    def __init__(
        self,
        responses: Optional[dict[str, str]] = None,
        *,
        fail_for_device: Optional[str] = None,
        delay_s: float = 0.02,
    ) -> None:
        self.responses = responses or {}
        self.calls: list[tuple[str, str]] = []
        self._fail_for_device = fail_for_device
        self._delay_s = delay_s

    def available(self) -> bool:
        return True

    async def run(
        self,
        *args: str,
        serial: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> AdbResult:
        await asyncio.sleep(self._delay_s)
        command = " ".join(args)
        self.calls.append((serial or "-", command))

        if serial is not None and serial == self._fail_for_device:
            return AdbResult(returncode=-1, stdout="", stderr="device offline", timed_out=True)

        for prefix, text in self.responses.items():
            if command.startswith(prefix):
                return AdbResult(returncode=0, stdout=text, stderr="")

        if command.startswith("install"):
            return AdbResult(returncode=0, stdout="Success\n", stderr="")
        if command.startswith("shell dumpsys package"):
            return AdbResult(
                returncode=0,
                stdout="    versionCode=42 minSdk=24 targetSdk=34\n    versionName=1.4.2\n",
                stderr="",
            )
        if command.startswith("uninstall"):
            return AdbResult(returncode=0, stdout="Success\n", stderr="")
        return AdbResult(returncode=0, stdout="", stderr="")
