"""Bounded authenticated JSON transport shared by explicit provider adapters."""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from contextvars import ContextVar
from http.client import HTTPException
import json
import math
import re
import ipaddress
import time
from urllib.parse import urlsplit
import urllib.error
import urllib.request
from uuid import uuid4


STREAM_PROGRESS = ContextVar("provider_stream_progress", default=None)


class ProviderError(RuntimeError):
    def __init__(self, kind, message, *, http_status=None, retry_after_seconds=None, provider_error_code=None, transport_details=None):
        super().__init__(message)
        self.kind, self.http_status = kind, http_status
        self.retry_after_seconds, self.provider_error_code = retry_after_seconds, provider_error_code
        self.transport_details = transport_details


def safe_transport_details(error, phase):
    """Allowlisted diagnostic metadata only: never exception messages/URLs/keys."""
    cause = error.reason if isinstance(error, urllib.error.URLError) else error
    def type_name(value):
        name = type(value).__name__
        return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,79}", name) else "TransportError"
    details = {"phase": phase if phase in {"connect_or_headers", "read_stream", "read_body"} else "unknown",
               "exception_type": type_name(error), "underlying_type": type_name(cause)}
    for attribute in ("errno", "verify_code"):
        number = getattr(cause, attribute, None)
        if isinstance(number, int) and not isinstance(number, bool):
            details[attribute] = number
    reason = getattr(cause, "reason", None)
    if isinstance(reason, str) and re.fullmatch(r"[A-Z0-9_]{1,80}", reason):
        details["ssl_reason"] = reason
    # CONNECT failures can be delivered as URLError strings rather than HTTPError.
    if isinstance(cause, str):
        match = re.search(r"Tunnel connection failed:\s*(\d{3})(?:\s|$)", cause)
        if match:
            details["proxy_tunnel_http_status"] = int(match.group(1))
    return details


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def validate_base_url(value):
    if not isinstance(value, str) or not value or any(c.isspace() for c in value):
        raise ProviderError("configuration", "Provider base URL must be a string.")
    try:
        parts = urlsplit(value)
        parts.port  # Validate an explicitly supplied port before any request is reserved.
    except ValueError:
        raise ProviderError("configuration", "Provider base URL has an invalid host or port.") from None
    try:
        loopback = parts.hostname == "localhost" or ipaddress.ip_address(parts.hostname or "").is_loopback
    except ValueError:
        loopback = False
    if (parts.scheme not in {"https", "http"} or parts.scheme == "http" and not loopback
            or not parts.hostname or parts.username or parts.password
            or parts.query or parts.fragment):
        raise ProviderError("configuration", "Use HTTPS, or HTTP on loopback only, without credentials, query or fragment.")
    return value.rstrip("/")


def validate_endpoint(value):
    if (not isinstance(value, str) or not value.startswith("/") or value.startswith("//")
            or any(c in value for c in ("?", "#", "\\", ":")) or any(c.isspace() for c in value)
            or ".." in value.split("/")):
        raise ProviderError("configuration", "Endpoint must be an absolute API path without query, fragment or traversal.")
    return value


class JSONClient:
    def __init__(self, api_key, *, base_url, provider_id, timeout=90, max_parallel=3,
                 on_event=None, reserve=None, transport=None, shared_semaphore=None,
                 max_request_bytes=16 * 1024 * 1024, request_gap_seconds=0):
        self.base_url = validate_base_url(base_url)
        if api_key is not None and (not isinstance(api_key, str) or not api_key.strip()):
            raise ProviderError("missing_key", "The selected provider needs a private API key.")
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or timeout <= 0
                or isinstance(max_parallel, bool) or not isinstance(max_parallel, int) or max_parallel < 1):
            raise ProviderError("configuration", "Provider time and concurrency limits must be positive.")
        self._key, self.provider_id, self.timeout = api_key, provider_id, timeout
        if isinstance(max_request_bytes, bool) or not isinstance(max_request_bytes, int) or max_request_bytes < 1:
            raise ProviderError("configuration", "Request byte limit must be a positive integer.")
        if (isinstance(request_gap_seconds, bool) or not isinstance(request_gap_seconds, (int, float))
                or not math.isfinite(request_gap_seconds) or request_gap_seconds < 0):
            raise ProviderError("configuration", "Request gap must be finite and nonnegative.")
        self.request_gap_seconds, self._last_request_finished = request_gap_seconds, None
        self.max_request_bytes = max_request_bytes
        self._semaphore = shared_semaphore or asyncio.Semaphore(max_parallel)
        self._provider_semaphore = asyncio.Semaphore(max_parallel) if shared_semaphore is not None else self._semaphore
        self._event, self._reserve = on_event or (lambda event: None), reserve or (lambda: None)
        self._transport = transport or self._http

    def _emit(self, event):
        def clean(value):
            if isinstance(value, str):
                return value.replace(self._key, "[REDACTED]") if self._key else value
            if isinstance(value, dict):
                return {clean(k): clean(v) for k, v in value.items()}
            if isinstance(value, (list, tuple)):
                return [clean(v) for v in value]
            return value
        self._event(clean(event))

    def _http(self, method, path, body):
        data = None if body is None else json.dumps(body, allow_nan=False).encode()
        headers = {"Content-Type": "application/json"}
        if body is not None and body.get("stream") is True:
            headers["Accept"] = "text/event-stream"
        if self._key:
            headers["Authorization"] = "Bearer " + self._key
        request = urllib.request.Request(
            self.base_url + path, data=data, method=method,
            headers=headers)
        phase = "connect_or_headers"
        try:
            with urllib.request.build_opener(_NoRedirect).open(request, timeout=self.timeout) as response:
                if body is not None and body.get("stream") is True:
                    if "text/event-stream" not in (response.headers.get("Content-Type") or ""):
                        raise ProviderError("invalid_response", "Expected a server-sent event stream.")
                    from .streaming import read_chat_stream
                    phase = "read_stream"
                    return read_chat_stream(response, progress=STREAM_PROGRESS.get())
                phase = "read_body"
                raw = response.read(16 * 1024 * 1024 + 1)
            if len(raw) > 16 * 1024 * 1024:
                raise ProviderError("invalid_response", "Provider response exceeds the local limit.")
            result = json.loads(raw)
            if not isinstance(result, dict):
                raise ValueError()
            json.dumps(result, allow_nan=False)
            return result
        except urllib.error.HTTPError as error:
            # Keep only bounded structured code and Retry-After metadata, not a
            # possibly sensitive error message, response headers or raw body.
            retry_after, provider_code = None, None
            try:
                header = error.headers.get("Retry-After") if error.headers else None
                if header is not None:
                    try:
                        seconds = float(header)
                        if math.isfinite(seconds) and seconds >= 0:
                            retry_after = seconds
                    except (ValueError, TypeError):
                        pass
                payload = error.read(4097)
                if len(payload) <= 4096:
                    detail = json.loads(payload)
                    detail = detail.get("error", {}) if isinstance(detail, dict) else {}
                    code = detail.get("code") if isinstance(detail, dict) else None
                    if isinstance(code, (str, int)) and not isinstance(code, bool):
                        text = str(code)
                        if 0 < len(text) <= 80 and all(c.isalnum() or c in "_.:-" for c in text):
                            provider_code = text
            except (OSError, ValueError, UnicodeError, HTTPException):
                pass
            finally:
                error.close()
            kinds = {400: "invalid_request", 401: "authentication", 403: "access_or_credit",
                     404: "unknown_model_or_path", 413: "request_limit", 422: "invalid_request",
                     429: "rate_limit", 503: "busy", 529: "busy"}
            raise ProviderError(kinds.get(error.code, "http_error"),
                                self.provider_id + " returned HTTP " + str(error.code),
                                http_status=error.code, retry_after_seconds=retry_after,
                                provider_error_code=provider_code) from None
        except (urllib.error.URLError, TimeoutError, OSError, HTTPException) as error:
            raise ProviderError("transport_uncertain", "Request failed or timed out; execution/billing may be unknown. No automatic retry.",
                                transport_details=safe_transport_details(error, phase)) from None
        except (ValueError, UnicodeError):
            raise ProviderError("invalid_response", "Provider returned invalid JSON.") from None

    async def request(self, method, path, body=None):
        validate_endpoint(path)
        if body is not None:
            try:
                encoded = json.dumps(body, allow_nan=False).encode()
            except (TypeError, ValueError):
                raise ProviderError("invalid_question", "Request must contain finite JSON.") from None
            if len(encoded) > self.max_request_bytes:
                raise ProviderError("request_limit", "Request exceeds the local byte limit.")
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(self._provider_semaphore)
            if self._provider_semaphore is not self._semaphore:
                await stack.enter_async_context(self._semaphore)
            if self._last_request_finished is not None and self.request_gap_seconds:
                wait = self._last_request_finished + self.request_gap_seconds - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
            if method == "POST":
                self._reserve()
            started, request_id = time.monotonic(), uuid4().hex
            metadata = {"request_id": request_id, "provider": self.provider_id,
                        "path": path, "model": (body or {}).get("model")}
            self._emit({"kind": "provider_request", **metadata, "request": body})
            loop = asyncio.get_running_loop()
            progress_token = STREAM_PROGRESS.set(lambda stats: loop.call_soon_threadsafe(
                self._emit, {"kind": "provider_stream_progress", **metadata, **stats}))
            try:
                result = await asyncio.to_thread(self._transport, method, path, body)
                if not isinstance(result, dict):
                    raise ProviderError("invalid_response", "Provider response must be an object.")
                try:
                    json.dumps(result, allow_nan=False)
                except (TypeError, ValueError):
                    raise ProviderError("invalid_response", "Provider response must contain finite JSON.") from None
                self._emit({"kind": "provider_response", **metadata,
                             "elapsed_seconds": time.monotonic() - started, "response": result})
                return result
            except ProviderError as error:
                self._emit({"kind": "provider_error", **metadata, "error_kind": error.kind,
                             "http_status": error.http_status,
                             "retry_after_seconds": error.retry_after_seconds,
                             "provider_error_code": error.provider_error_code,
                             "transport_details": error.transport_details,
                             "elapsed_seconds": time.monotonic() - started})
                raise
            finally:
                self._last_request_finished = time.monotonic()
                STREAM_PROGRESS.reset(progress_token)
