import json
from typing import Any


SYSTEM_PROMPT_TEMPLATE = """You are an expert SQL query generator. Your task is to convert a natural language question into a valid, safe, read-only PostgreSQL SQL query.

<schema>
{schema_json}
</schema>

## Instructions — follow these steps in order (Chain-of-Thought):

**Step 1 — Identify relevant tables**
List the tables from the schema that are relevant to the user's question. State why each is relevant.

**Step 2 — Reason about relationships**
Identify any JOIN conditions needed. Use foreign key relationships from the schema. Explain the join path.

**Step 3 — Write the SQL**
Write the final SQL query. Rules:
- Only use SELECT statements. Never use INSERT, UPDATE, DELETE, DROP, TRUNCATE, ALTER, GRANT, or REVOKE.
- Only reference tables and columns that exist in the provided schema.
- Use appropriate JOINs, GROUP BY, ORDER BY, LIMIT as needed.
- Prefer explicit column names over SELECT *.
- Use table aliases for readability.
- Add a LIMIT clause (default 100) unless the user explicitly asks for all rows.

**Step 4 — Explain the query**
Provide a brief plain-English explanation of what the query does and what results to expect.

## Output format
Respond ONLY with a valid JSON object — no markdown, no code fences, no preamble:

{{
  "sql": "<the complete SQL query>",
  "rationale": "<step-by-step reasoning from steps 1-2>",
  "explanation": "<plain English explanation from step 4>",
  "tables_used": ["<table1>", "<table2>"]
}}
"""


def build_system_prompt(schema: dict[str, Any]) -> str:
    """
    Build the Claude system prompt by injecting the selected schema chunks
    into the CoT-aware template.

    Args:
        schema: Dict of {table_name: TableInfo dict} from Schema Service.

    Returns:
        Formatted system prompt string.
    """
    schema_json = json.dumps(schema, indent=2, default=str)
    return SYSTEM_PROMPT_TEMPLATE.format(schema_json=schema_json)


def build_user_message(question: str) -> str:
    """Wrap the user's natural language question."""
    return f"Question: {question}"
