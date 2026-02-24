"""
utils/retry.py — Exponential-backoff retry decorator for async HTTP calls.

Uses tenacity under the hood. Logs each attempt with structlog so failures
are observable without crashing the pipeline.

Usage:
    from candata_pipeline.utils.retry import with_retry

    @with_retry(max_attempts=3, base_delay=1.0)
    async def fetch_data(url: str) -> bytes:
        async with httpx.AsyncClient() as client:
            r = await client.get(url)
            r.raise_for_status()
            return r.content

    # Default decorator (3 attempts, 1/2/4 s delays, retries on any Exception)
    @with_retry()
    async def call_api() -> dict: ...
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

import structlog
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = structlog.get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Coroutine[Any, Any, Any]])


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retry_on: type[Exception] | tuple[type[Exception], ...] = Exception,
) -> Callable[[F], F]:
    """
    Decorator that retries an async function with exponential backoff.

    Delays: base_delay * 2^(attempt-1), capped at max_delay.
    Default: 1 s, 2 s, 4 s.

    Args:
        max_attempts: Total attempts before raising.
        base_delay:   Initial delay in seconds.
        max_delay:    Maximum delay cap in seconds.
        retry_on:     Exception type(s) that trigger a retry.

    Returns:
        Decorated async function.
    """

    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt_log = log.bind(function=fn.__qualname__)
            try:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(max_attempts),
                    wait=wait_exponential(multiplier=base_delay, max=max_delay),
                    retry=retry_if_exception_type(retry_on),
                    reraise=True,
                ):
                    with attempt:
                        attempt_num = attempt.retry_state.attempt_number
                        if attempt_num > 1:
                            attempt_log.warning(
                                "retry_attempt",
                                attempt=attempt_num,
                                max_attempts=max_attempts,
                                last_error=str(attempt.retry_state.outcome.exception())
                                if attempt.retry_state.outcome
                                else None,
                            )
                        return await fn(*args, **kwargs)
            except RetryError as exc:
                attempt_log.error(
                    "retry_exhausted",
                    max_attempts=max_attempts,
                    error=str(exc),
                )
                raise
            except Exception as exc:
                attempt_log.error("call_failed", error=str(exc), exc_info=True)
                raise

        return wrapper  # type: ignore[return-value]

    return decorator


def with_retry_sync(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retry_on: type[Exception] | tuple[type[Exception], ...] = Exception,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Same as with_retry but for synchronous functions.
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt_log = log.bind(function=fn.__qualname__)
            last_exc: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except retry_on as exc:
                    last_exc = exc
                    if attempt < max_attempts:
                        delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                        attempt_log.warning(
                            "retry_attempt",
                            attempt=attempt,
                            max_attempts=max_attempts,
                            delay_s=delay,
                            error=str(exc),
                        )
                        asyncio.get_event_loop().run_until_complete(asyncio.sleep(delay))
            attempt_log.error("retry_exhausted", max_attempts=max_attempts)
            raise last_exc  # type: ignore[misc]

        return wrapper

    return decorator
