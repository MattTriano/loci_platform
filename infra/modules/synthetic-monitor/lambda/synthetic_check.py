"""Synthetic monitor — checks the health of every monitored target.

Triggered by EventBridge every 5 minutes. For each target listed in
STATIC_SITE_TARGETS and ROUTING_API_TARGETS env vars, makes an HTTP
check and records the result. Writes a single status.json to S3 with
all results.

Phase 1 (this file): writes results to S3 only.
Phase 3 (future): also emits CloudWatch custom metrics for alarms.
"""

import base64
import concurrent.futures
import json
import os
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime

import boto3

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

CHECK_TIMEOUT_SECONDS = 10
SSM_CACHE: dict[str, str] = {}


def get_ssm(path: str) -> str | None:
    """Read an SSM parameter, caching for the duration of the Lambda invocation.

    Returns None if the parameter doesn't exist (e.g. a city without a
    synthetic fixture yet).
    """
    if path in SSM_CACHE:
        return SSM_CACHE[path]
    try:
        resp = ssm.get_parameter(Name=path, WithDecryption=True)
        value = resp["Parameter"]["Value"]
        SSM_CACHE[path] = value
        return value
    except ssm.exceptions.ParameterNotFound:
        return None


def check_static(target: dict, basic_auth_header: str | None) -> dict:
    """Check a static site target with a GET, optionally with basic auth."""
    headers = {}
    if basic_auth_header:
        headers["Authorization"] = basic_auth_header

    req = urllib.request.Request(target["url"], headers=headers, method="GET")
    return _do_request(target, req)


def check_routing(target: dict) -> dict:
    """Check a routing API target with a POST.

    Reads the API key and fixture from SSM. Skips the check (returns
    'skipped' status) if the fixture isn't set yet.
    """
    fixture_json = get_ssm(target["fixture_ssm_path"])
    if fixture_json is None:
        return {
            **_result_envelope(target),
            "status": "skipped",
            "error": "No synthetic fixture set in SSM yet",
        }

    api_key = get_ssm(target["api_key_ssm_path"])
    if api_key is None:
        return {
            **_result_envelope(target),
            "status": "failed",
            "error": "Could not read routing API key from SSM",
        }

    req = urllib.request.Request(
        target["url"],
        data=fixture_json.encode("utf-8"),
        headers={"Content-Type": "application/json", "X-Api-Key": api_key},
        method="POST",
    )
    return _do_request(target, req)


def _do_request(target: dict, req: urllib.request.Request) -> dict:
    """Run a request and return a normalized result dict."""
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=CHECK_TIMEOUT_SECONDS) as resp:
            body = resp.read()
            elapsed_ms = int((time.monotonic() - started) * 1000)
            status_ok = 200 <= resp.status < 300 and len(body) > 0
            return {
                **_result_envelope(target),
                "status": "ok" if status_ok else "failed",
                "http_status": resp.status,
                "latency_ms": elapsed_ms,
                "error": None if status_ok else f"Unexpected status {resp.status} or empty body",
            }
    except urllib.error.HTTPError as e:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        return {
            **_result_envelope(target),
            "status": "failed",
            "http_status": e.code,
            "latency_ms": elapsed_ms,
            "error": f"HTTP {e.code}: {e.reason}",
        }
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        return {
            **_result_envelope(target),
            "status": "failed",
            "http_status": None,
            "latency_ms": elapsed_ms,
            "error": str(e),
        }


def _result_envelope(target: dict) -> dict:
    return {
        "name": target["name"],
        "type": "static" if "fixture_ssm_path" not in target else "routing",
        "url": target["url"],
        "http_status": None,
        "latency_ms": None,
        "error": None,
    }


def lambda_handler(event, context):
    static_targets = json.loads(os.environ["STATIC_SITE_TARGETS"])
    routing_targets = json.loads(os.environ["ROUTING_API_TARGETS"])
    status_bucket = os.environ["STATUS_BUCKET"]
    status_key = os.environ["STATUS_KEY"]
    environment = os.environ["ENVIRONMENT"]

    basic_auth_ssm_path = os.environ.get("BASIC_AUTH_SSM_PATH", "")
    basic_auth_header = None
    if basic_auth_ssm_path:
        credentials = get_ssm(basic_auth_ssm_path)
        if credentials:
            basic_auth_header = "Basic " + base64.b64encode(credentials.encode()).decode()

    checks = []
    for t in static_targets:
        checks.append(("static", t))
    for t in routing_targets:
        checks.append(("routing", t))

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(checks)) as executor:
        futures = {}
        for check_type, target in checks:
            if check_type == "static":
                fut = executor.submit(check_static, target, basic_auth_header)
            else:
                fut = executor.submit(check_routing, target)
            futures[fut] = target["name"]

        results = []
        for fut in concurrent.futures.as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                results.append(
                    {
                        "name": futures[fut],
                        "type": "unknown",
                        "status": "failed",
                        "error": f"Check raised exception: {e}",
                        "http_status": None,
                        "latency_ms": None,
                        "url": None,
                    }
                )

    results.sort(key=lambda r: r["name"])

    status_doc = {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "environment": environment,
        "targets": results,
    }

    s3.put_object(
        Bucket=status_bucket,
        Key=status_key,
        Body=json.dumps(status_doc, indent=2).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-cache, max-age=0",
    )

    return {
        "checks_run": len(results),
        "ok": sum(1 for r in results if r["status"] == "ok"),
        "failed": sum(1 for r in results if r["status"] == "failed"),
        "skipped": sum(1 for r in results if r["status"] == "skipped"),
    }
