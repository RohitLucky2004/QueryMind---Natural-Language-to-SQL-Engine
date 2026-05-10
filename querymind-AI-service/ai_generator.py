import json
import logging
import time
from typing import Any, AsyncGenerator

import anthropic

from core.config import settings
from core.prompt_builder import build_system_prompt, build_user_message
from models.query_models import GenerationResult, ValidationResult
from services.safety import check_operation_safety
from services.schema_validator import validate_against_schema
from services.injection_guard import check_injection_patterns

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 2048


def _get_client() -> anthropic.Anthropic:
    return anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)


def _parse_claude_response(content: str) -> dict[str, Any]:
    """
    Parse the structured JSON response from Claude.
    Strips markdown fences if the model wraps the JSON.
    """
    text = content.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last fence lines
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    return json.loads(text)


def _run_validation(
    sql: str,
    question: str,
    schema: dict[str, Any],
) -> ValidationResult:
    """
    Run all three validation passes in order (fail-fast).

    Pass 1 — safety regex (cheapest)
    Pass 2 — sqlglot AST + schema cross-reference
    Pass 3 — injection pattern detection on question text
    """
    # Pass 1
    passed, reason = check_operation_safety(sql)
    if not passed:
        return ValidationResult(passed=False, failed_pass=1, reason=reason)

    # Pass 2
    passed, reason, invalid_refs = validate_against_schema(sql, schema)
    if not passed:
        return ValidationResult(
            passed=False,
            failed_pass=2,
            reason=reason,
            invalid_references=invalid_refs,
        )

    # Pass 3
    passed, reason = check_injection_patterns(question)
    if not passed:
        return ValidationResult(passed=False, failed_pass=3, reason=reason)

    return ValidationResult(passed=True)


def generate_sql(
    question: str,
    schema: dict[str, Any],
) -> GenerationResult:
    """
    Call Claude to generate SQL from a natural language question and schema context.
    Runs three-pass validation on the result.

    Args:
        question: Natural language question from the user.
        schema: Relevant schema chunks from Schema Service.

    Returns:
        GenerationResult with validated SQL and metadata.

    Raises:
        ValueError: If validation fails.
        anthropic.APIError: On Claude API errors.
    """
    client = _get_client()
    system_prompt = build_system_prompt(schema)
    user_message = build_user_message(question)

    logger.info("Calling Claude for SQL generation (question: %.100s)", question)
    start_time = time.monotonic()

    response = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    )

    latency_ms = int((time.monotonic() - start_time) * 1000)
    tokens_used = response.usage.input_tokens + response.usage.output_tokens
    raw_content = response.content[0].text

    logger.info(
        "Claude response received — tokens=%d latency_ms=%d",
        tokens_used,
        latency_ms,
    )

    try:
        parsed = _parse_claude_response(raw_content)
    except (json.JSONDecodeError, KeyError) as e:
        raise ValueError(f"Claude returned unparseable response: {e}\nRaw: {raw_content[:500]}")

    sql = parsed.get("sql", "").strip()
    if not sql:
        raise ValueError("Claude returned empty SQL")

    # Run three-pass validation
    validation = _run_validation(sql, question, schema)
    if not validation.passed:
        raise ValueError(
            f"SQL validation failed (pass {validation.failed_pass}): {validation.reason}"
        )

    return GenerationResult(
        sql=sql,
        rationale=parsed.get("rationale", ""),
        explanation=parsed.get("explanation", ""),
        tables_used=parsed.get("tables_used", []),
        tokens_used=tokens_used,
        latency_ms=latency_ms,
        validation=validation.model_dump(),
    )


async def stream_sql_tokens(
    question: str,
    schema: dict[str, Any],
) -> AsyncGenerator[str, None]:
    """
    Stream SQL generation tokens from Claude for the WebSocket endpoint.

    Yields each token chunk as it arrives. After streaming, yields a final
    JSON frame with the complete validated result.

    Args:
        question: Natural language question.
        schema: Relevant schema chunks.

    Yields:
        str — token chunks, then a terminal JSON frame prefixed with '__RESULT__:'
    """
    client = _get_client()
    system_prompt = build_system_prompt(schema)
    user_message = build_user_message(question)

    full_text = ""
    tokens_used = 0
    start_time = time.monotonic()

    with client.messages.stream(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    ) as stream:
        for chunk in stream.text_stream:
            full_text += chunk
            yield chunk

        # Get final usage after stream completes
        final_msg = stream.get_final_message()
        tokens_used = (
            final_msg.usage.input_tokens + final_msg.usage.output_tokens
        )

    latency_ms = int((time.monotonic() - start_time) * 1000)

    # Parse and validate the completed streamed response
    try:
        parsed = _parse_claude_response(full_text)
        sql = parsed.get("sql", "").strip()

        validation = _run_validation(sql, question, schema)
        result = {
            "success": validation.passed,
            "sql": sql if validation.passed else None,
            "rationale": parsed.get("rationale", ""),
            "explanation": parsed.get("explanation", ""),
            "tables_used": parsed.get("tables_used", []),
            "tokens_used": tokens_used,
            "latency_ms": latency_ms,
            "validation": validation.model_dump(),
            "error": None if validation.passed else validation.reason,
        }
    except Exception as e:
        result = {
            "success": False,
            "sql": None,
            "error": str(e),
            "tokens_used": tokens_used,
            "latency_ms": latency_ms,
        }

    # Terminal frame with the complete result for the Gateway to use
    yield f"__RESULT__:{json.dumps(result)}"
