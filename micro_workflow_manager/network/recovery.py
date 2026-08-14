from __future__ import annotations

import asyncio
import gzip
import json
import time
import zlib
from typing import Any

import httpx

from .types import ClientShard, CohortStreamStall, NetworkRequest


class NetworkRecoveryMixin:
    @staticmethod
    def _read_timeout_seconds(kwargs: dict[str, Any]) -> float | None:
        timeout = kwargs.get("timeout")
        value = getattr(timeout, "read", None)
        if isinstance(value, (int, float)) and value > 0:
            return float(value)
        return None

    @staticmethod
    def _complete_json_document(content: bytes, content_encoding: str) -> bool:
        """Return true only when the complete encoded JSON entity is present."""
        try:
            encoding = content_encoding.strip().lower()
            if encoding in {"", "identity"}:
                decoded = content
            elif encoding == "gzip":
                decoded = gzip.decompress(content)
            elif encoding == "deflate":
                decoded = zlib.decompress(content)
            else:
                return False
            json.loads(decoded)
        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            EOFError,
            OSError,
            zlib.error,
        ):
            return False
        return True

    async def _request_with_progress(
        self,
        request: NetworkRequest,
        shard: ClientShard,
        kwargs: dict[str, Any],
        active: dict[str, Any],
    ) -> httpx.Response:
        """Read JSON without depending forever on HTTP/2 END_STREAM."""
        if not request.expect_json:
            return await shard.client.request(request.method, request.url, **kwargs)

        async with shard.client.stream(request.method, request.url, **kwargs) as source:
            response_headers_at = time.monotonic()
            read_timeout = self._read_timeout_seconds(kwargs)
            active["response_headers_at"] = response_headers_at
            generation_id = source.headers.get("x-generation-id")
            if generation_id:
                active["generation_id"] = str(generation_id)
            # Custom transports may return an already-buffered terminal body.
            if source.is_stream_consumed:
                return source
            body = bytearray()
            iterator = source.aiter_raw().__aiter__()
            ended = False
            json_complete = False
            next_chunk: asyncio.Task | None = None
            while True:
                try:
                    next_chunk = asyncio.create_task(iterator.__anext__())
                    while True:
                        timeout = (
                            self._json_terminal_grace_seconds
                            if json_complete
                            else min(5.0, self._cohort_stall_seconds / 4.0)
                        )
                        if read_timeout is not None and not json_complete:
                            idle_since = float(
                                active.get("last_response_progress_at")
                                or response_headers_at
                            )
                            timeout = min(
                                timeout,
                                max(0.001, idle_since + read_timeout - time.monotonic()),
                            )
                        done, _ = await asyncio.wait({next_chunk}, timeout=timeout)
                        if done:
                            chunk = next_chunk.result()
                            next_chunk = None
                            break
                        now_value = time.monotonic()
                        if json_complete:
                            next_chunk.cancel()
                            await asyncio.gather(next_chunk, return_exceptions=True)
                            next_chunk = None
                            raise asyncio.TimeoutError()
                        stall_reason = self._cohort_stream_stall_reason(
                            active, shard, now_value
                        )
                        if stall_reason is not None:
                            next_chunk.cancel()
                            await asyncio.gather(next_chunk, return_exceptions=True)
                            next_chunk = None
                            raise CohortStreamStall(stall_reason)
                        if read_timeout is not None:
                            idle_since = float(
                                active.get("last_response_progress_at")
                                or response_headers_at
                            )
                            if now_value - idle_since >= read_timeout:
                                next_chunk.cancel()
                                await asyncio.gather(
                                    next_chunk, return_exceptions=True
                                )
                                next_chunk = None
                                raise httpx.ReadTimeout(
                                    "response body made no progress within the "
                                    f"configured {read_timeout:g}s read timeout",
                                    request=source.request,
                                )
                except StopAsyncIteration:
                    ended = True
                    break
                except asyncio.TimeoutError:
                    break
                body.extend(chunk)
                active["response_bytes"] = len(body)
                active["last_response_progress_at"] = time.monotonic()
                if source.headers.get("content-type", "").lower().startswith(
                    "application/json"
                ):
                    json_complete = self._complete_json_document(
                        bytes(body), source.headers.get("content-encoding", "")
                    )
                if not json_complete:
                    stall_reason = self._cohort_stream_stall_reason(
                        active, shard, time.monotonic()
                    )
                    if stall_reason is not None:
                        raise CohortStreamStall(stall_reason)

            if json_complete and not ended:
                shard.retiring = True
                shard.retired_reason = (
                    "complete JSON arrived without HTTP/2 stream termination"
                )
                shard.retired_at = time.monotonic()
                self._json_stream_recoveries += 1
                active["json_completed_without_terminal"] = True

            response = httpx.Response(
                source.status_code,
                headers=source.headers,
                content=bytes(body),
                extensions=dict(source.extensions),
                request=source.request,
            )
            if generation_id:
                response.extensions["mwf_generation_id"] = str(generation_id)
            return response

    def _cohort_stream_stall_reason(
        self,
        active: dict[str, Any],
        shard: ClientShard,
        now_value: float,
    ) -> str | None:
        """Prove one stream is a nonterminal outlier using its live cohort."""
        attempt_started = float(active["attempt_started_at"])
        sibling_terminals = (
            shard.requests_completed
            + shard.requests_failed
            - int(active["cohort_terminal_baseline"])
        )
        # At peak load a fixed evidence floor avoids reacting to ordinary
        # generation variance.  During a quiet tail there may be fewer peers
        # than that floor.  If every available same-shard peer has terminated,
        # two or more terminals are sufficient proof that the remaining stream
        # is the cohort outlier; requiring an impossible fixed count would leave
        # it to expire at the caller lease instead.
        available_siblings = max(0, shard.in_flight - 1) + sibling_terminals
        terminal_evidence = min(
            self._cohort_terminal_evidence,
            max(2, available_siblings),
        )
        age = now_value - attempt_started
        if (
            age >= self._cohort_stall_seconds
            and sibling_terminals >= terminal_evidence
            and shard.last_terminal_at > attempt_started
        ):
            return (
                f"stream remained nonterminal for {age:.1f}s while "
                f"{sibling_terminals} newer sibling requests terminated "
                f"on live shard {shard.shard_id}"
            )
        return None
