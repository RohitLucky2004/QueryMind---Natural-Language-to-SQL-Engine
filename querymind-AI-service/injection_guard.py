import re
import logging

logger = logging.getLogger(__name__)

# Patterns that suggest SQL injection embedded in the natural language question
INJECTION_PATTERNS = [
    re.compile(r";\s*(DROP|DELETE|INSERT|UPDATE|ALTER|TRUNCATE)", re.IGNORECASE),
    re.compile(r"UNION\s+SELECT", re.IGNORECASE),
    re.compile(r"--\s*$", re.MULTILINE),          # SQL line comment at end
    re.compile(r"/\*.*?\*/", re.DOTALL),           # Block comment
    re.compile(r"'\s*OR\s*'?\d*\s*'?\s*=\s*'?\d", re.IGNORECASE),  # ' OR '1'='1
    re.compile(r"xp_cmdshell", re.IGNORECASE),
    re.compile(r"EXEC\s*\(", re.IGNORECASE),
    re.compile(r"WAITFOR\s+DELAY", re.IGNORECASE),
    re.compile(r"BENCHMARK\s*\(", re.IGNORECASE),
    re.compile(r"SLEEP\s*\(", re.IGNORECASE),
]


def check_injection_patterns(question: str) -> tuple[bool, str | None]:
    """
    Pass 3: Scan the user's original natural language question for SQL injection
    patterns embedded in the question text.

    This operates on the question, NOT the generated SQL — it catches adversarial
    inputs that attempt to escape the prompt context and inject raw SQL.

    Args:
        question: The raw natural language question from the user.

    Returns:
        (passed, reason) — passed=True means clean, False means suspicious input.
    """
    for pattern in INJECTION_PATTERNS:
        if pattern.search(question):
            reason = (
                f"Potential SQL injection pattern detected in question input: "
                f"matched /{pattern.pattern}/"
            )
            logger.warning(
                "Pass 3 FAILED — injection pattern in question: %.200s", question
            )
            return False, reason

    logger.debug("Pass 3 passed — no injection patterns in question")
    return True, None
