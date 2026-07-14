"""Tool registry: Pydantic schemas, timeouts, error taxonomy.

A malformed LLM tool call becomes a tool_failed event with a structured
validation error — never an exception escaping the worker. Classification
(retryable vs terminal) lives here, at the tool boundary; the engine only
sees its two step-error types.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from pydantic import BaseModel, ValidationError

from ..engine.events import EventType
from ..engine.worker import RetryableStepError, TerminalStepError
from .llm import Recorder


class RetryableToolError(RetryableStepError):
    """Network, 429s, timeouts — backoff and retry."""


class TerminalToolError(TerminalStepError):
    """Validation, 4xx semantics, business rules — scheduler decides."""


@dataclass
class Tool:
    name: str
    description: str
    input_model: type[BaseModel]
    fn: Callable[..., Awaitable[Any]]
    timeout_s: float = 30.0


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(
        self,
        description: str,
        input_model: type[BaseModel],
        timeout_s: float = 30.0,
        name: str | None = None,
    ):
        def deco(fn: Callable[..., Awaitable[Any]]):
            tool_name = name or fn.__name__
            self._tools[tool_name] = Tool(
                tool_name, description, input_model, fn, timeout_s
            )
            return fn

        return deco

    def anthropic_tools(self) -> list[dict[str, Any]]:
        return [
            {
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_model.model_json_schema(),
            }
            for t in self._tools.values()
        ]

    async def call(self, rec: Recorder, name: str, args: dict[str, Any]) -> Any:
        """Validate, execute, record. Replays a recorded success without
        re-executing; a recorded failure is re-attempted."""
        key = rec.key("tool", name)
        prior = rec.recorded(key)
        if prior is not None and prior.get("outcome") == "succeeded":
            return prior["result"]

        base = {"record_key": key, "step_id": rec.ctx.step_id, "tool": name}

        async def fail(error: dict[str, Any], exc: Exception) -> Exception:
            await rec.record(
                EventType.TOOL_FAILED, base | {"outcome": "failed", "error": error}
            )
            return exc

        tool = self._tools.get(name)
        if tool is None:
            raise await fail(
                {"kind": "unknown_tool", "message": name},
                TerminalToolError(f"unknown tool {name}"),
            )
        try:
            parsed = tool.input_model(**args)
        except ValidationError as e:
            raise await fail(
                {"kind": "validation", "errors": e.errors(include_url=False)},
                TerminalToolError(f"invalid args for {name}"),
            )

        await rec.record(EventType.TOOL_CALLED, base | {"args": args})
        try:
            async with asyncio.timeout(tool.timeout_s):
                result = await tool.fn(parsed, rec.ctx)
        except (RetryableToolError, TerminalToolError) as e:
            raise await fail({"kind": type(e).__name__, "message": str(e)}, e)
        except TimeoutError as e:
            raise await fail(
                {"kind": "timeout", "timeout_s": tool.timeout_s},
                RetryableToolError(f"{name} timed out after {tool.timeout_s}s"),
            ) from e
        except Exception as e:  # unclassified -> retryable, capped by the engine
            raise await fail(
                {"kind": type(e).__name__, "message": str(e)},
                RetryableToolError(f"{name}: {e}"),
            ) from e

        await rec.record(
            EventType.TOOL_SUCCEEDED,
            base | {"outcome": "succeeded", "args": args, "result": result},
        )
        return result
