# The image ships the orchestrator, not the devices.
#
# adb talks to phones over USB or TCP. USB passthrough into a container is
# possible but fragile and host-specific, so the intended deployment is:
# containerised orchestrator, phones reached over TCP/tunnel, and one host
# agent per physical bench that keeps USB devices attached to its own adb
# server. `--network host` is the simplest way to reach a local adb server;
# ADB_SERVER_SOCKET points at a remote one.

FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# android-tools-adb is the only system dependency, and it is genuinely optional:
# a web-only deployment never calls it, and the health monitor degrades to "no
# opinion" when it is missing rather than failing closed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends android-tools-adb curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

# Non-root: nothing here needs privileges, and adb over TCP does not either.
RUN useradd --create-home --uid 10001 orchestrator \
    && chown -R orchestrator:orchestrator /app
USER orchestrator

EXPOSE 8080

# /healthz answers as soon as the process is up; `ready` in its body is the
# separate question of whether any device is assignable. The container is
# healthy when the process serves, not when the fleet happens to be populated --
# otherwise an empty bench restart-loops the orchestrator.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["python", "cli.py"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8080"]
