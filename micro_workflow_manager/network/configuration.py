from __future__ import annotations

import os
import ssl
from typing import Any

import httpx


def _positive_integer(value, environment: str, default: int) -> int:
    if value is None:
        try:
            value = int(os.getenv(environment, str(default)))
        except ValueError as error:
            raise ValueError(f"{environment} must be an integer >= 1") from error
    if type(value) is not int or value < 1:
        raise ValueError(f"{environment.lower()} must be an integer >= 1")
    return value


def _positive_float(environment: str, default: float) -> float:
    try:
        value = float(os.getenv(environment, str(default)))
    except ValueError as error:
        raise ValueError(f"{environment} must be a positive number") from error
    if value <= 0:
        raise ValueError(f"{environment} must be a positive number")
    return value


def _nonnegative_integer(environment: str, default: int) -> int:
    try:
        value = int(os.getenv(environment, str(default)))
    except ValueError as error:
        raise ValueError(f"{environment} must be an integer >= 0") from error
    if value < 0:
        raise ValueError(f"{environment} must be an integer >= 0")
    return value


class NetworkConfigurationMixin:
    def configure(self, *, http2=False, streams_per_connection=100,
                  http2_stream_safety_cap=None,
                  http1_connections_per_shard=None, architecture=None,
                  state_flush_interval=2.0, tcp_keepalive=None,
                  tcp_keepalive_idle_seconds=None,
                  tcp_keepalive_interval_seconds=None,
                  tcp_keepalive_probes=None, **client_kwargs: Any) -> None:
        if type(http2) is not bool:
            raise ValueError("http2 must be a bool")
        if type(streams_per_connection) is not int or streams_per_connection < 1:
            raise ValueError("streams_per_connection must be an integer >= 1")
        http2_stream_safety_cap = _positive_integer(
            http2_stream_safety_cap, "MWF_HTTP2_STREAM_SAFETY_CAP", 32
        )
        http1_connections_per_shard = _positive_integer(
            http1_connections_per_shard, "MWF_HTTP1_CONNECTIONS_PER_SHARD", 16
        )
        if tcp_keepalive is None:
            tcp_keepalive = os.getenv("MWF_TCP_KEEPALIVE", "1").strip().lower() not in {
                "0", "false", "no", "off",
            }
        if type(tcp_keepalive) is not bool:
            raise ValueError("tcp_keepalive must be a bool")
        tcp_keepalive_idle_seconds = _positive_integer(
            tcp_keepalive_idle_seconds, "MWF_TCP_KEEPALIVE_IDLE_SECONDS", 30
        )
        tcp_keepalive_interval_seconds = _positive_integer(
            tcp_keepalive_interval_seconds, "MWF_TCP_KEEPALIVE_INTERVAL_SECONDS", 10
        )
        tcp_keepalive_probes = _positive_integer(
            tcp_keepalive_probes, "MWF_TCP_KEEPALIVE_PROBES", 3
        )
        json_terminal_grace_seconds = _positive_float(
            "MWF_JSON_TERMINAL_GRACE_SECONDS", 5.0
        )
        cohort_stall_seconds = _positive_float(
            "MWF_HTTP2_COHORT_STALL_SECONDS", 300.0
        )
        cohort_terminal_evidence = _nonnegative_integer(
            "MWF_HTTP2_COHORT_TERMINALS", 16
        )
        if cohort_terminal_evidence < 1:
            raise ValueError("MWF_HTTP2_COHORT_TERMINALS must be an integer >= 1")
        cohort_retry_limit = _nonnegative_integer("MWF_HTTP2_COHORT_RETRIES", 2)
        transport_error_retry_limit = _nonnegative_integer(
            "MWF_HTTP_TRANSPORT_RETRIES", 2
        )
        architecture = str(
            architecture or os.getenv("MWF_NETWORK_ARCHITECTURE", "manager")
        ).strip().lower()
        architecture = {"legacy": "direct", "central": "manager"}.get(
            architecture, architecture
        )
        if architecture not in {"manager", "direct"}:
            raise ValueError("network architecture must be 'manager' or 'direct'")
        state_flush_interval = float(state_flush_interval)
        if not 0 < state_flush_interval <= 2.0:
            raise ValueError("state_flush_interval must be > 0 and <= 2 seconds")

        normalized_client_kwargs = dict(client_kwargs)
        verify = normalized_client_kwargs.get("verify", True)
        if "transport" not in normalized_client_kwargs and not isinstance(
            verify, ssl.SSLContext
        ):
            verify = normalized_client_kwargs.pop("verify", True)
            cert = normalized_client_kwargs.pop("cert", None)
            normalized_client_kwargs["verify"] = httpx.create_ssl_context(
                verify=verify,
                cert=cert,
                trust_env=normalized_client_kwargs.get("trust_env", True),
            )

        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("shared network manager is already active")
            self._client_kwargs = normalized_client_kwargs
            self._http2 = http2
            self._requested_streams_per_connection = streams_per_connection
            self._http2_stream_safety_cap = http2_stream_safety_cap
            self._streams_per_connection = (
                min(streams_per_connection, http2_stream_safety_cap)
                if http2 else streams_per_connection
            )
            self._http1_connections_per_shard = http1_connections_per_shard
            self._tcp_keepalive = tcp_keepalive
            self._tcp_keepalive_idle_seconds = tcp_keepalive_idle_seconds
            self._tcp_keepalive_interval_seconds = tcp_keepalive_interval_seconds
            self._tcp_keepalive_probes = tcp_keepalive_probes
            self._json_terminal_grace_seconds = json_terminal_grace_seconds
            self._cohort_stall_seconds = cohort_stall_seconds
            self._cohort_terminal_evidence = cohort_terminal_evidence
            self._cohort_retry_limit = cohort_retry_limit
            self._transport_error_retry_limit = transport_error_retry_limit
            self._architecture = architecture
            self._state_flush_interval = state_flush_interval
