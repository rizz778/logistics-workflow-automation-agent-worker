import re
import json
import httpx
from typing import Dict, Any, List, Optional
from core.config import settings
from core.logging import logger
from db.pool import get_db_connection
from prompts.nl_query_prompts import (
    POSTGRES_SCHEMA_PROMPT,
    NL_QUERY_SYNTHESIS_SYSTEM_PROMPT,
    PROMPT_INJECTION_PATTERNS,
    FORBIDDEN_SQL_PATTERNS,
    format_synthesis_user_prompt
)

class NaturalLanguageQueryService:
    """Service translating plain English questions to safe PostgreSQL queries with injection prevention."""

    def sanitize_user_input(self, user_query: str) -> str:
        """Validates and sanitizes user input against prompt injection patterns."""
        clean_query = user_query.strip()
        for pattern in PROMPT_INJECTION_PATTERNS:
            if re.search(pattern, clean_query, re.IGNORECASE):
                logger.warning(f"Blocked potential prompt injection query: {clean_query}")
                raise ValueError("Query rejected: Contains restricted or malicious instruction patterns.")
        return clean_query

    def validate_and_harden_sql(self, sql_query: str) -> str:
        """Deterministic security shield: strictly verifies query is a benign read-only SELECT."""
        clean_sql = sql_query.strip()

        # Remove markdown wrappers if LLM emitted them
        if clean_sql.startswith("```"):
            lines = clean_sql.splitlines()
            if lines[0].startswith("```sql") or lines[0].startswith("```"):
                clean_sql = "\n".join(lines[1:-1]).strip()

        # 1. Prohibit stacked queries (no semicolons allowed)
        if ";" in clean_sql.rstrip(";"):
            raise ValueError("Stacked queries with semicolons are strictly prohibited.")
        clean_sql = clean_sql.rstrip(";").strip()

        # 2. Enforce strict SELECT prefix
        if not clean_sql.lower().startswith("select"):
            raise ValueError("Only read-only SELECT queries are permitted.")

        # 3. Check for forbidden keywords (data modification or system catalog access)
        for pattern in FORBIDDEN_SQL_PATTERNS:
            if re.search(pattern, clean_sql, re.IGNORECASE):
                raise ValueError("Query rejected: Prohibited SQL command or system table detected.")

        # 4. Enforce Limit Clause to prevent memory exhaustion DoS
        if "limit" not in clean_sql.lower():
            clean_sql += " LIMIT 50"

        return clean_sql

    async def _call_gemini_api(self, prompt: str, system_instruction: str) -> str:
        """Calls Gemini API using raw async HTTP to avoid heavy dependency bloat."""
        if not settings.gemini_api_key:
            raise ValueError("GEMINI_API_KEY is not configured in .env.")

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{settings.gemini_model}:generateContent?key={settings.gemini_api_key}"
        
        payload = {
            "contents": [
                {"role": "user", "parts": [{"text": prompt}]}
            ],
            "systemInstruction": {
                "parts": [{"text": system_instruction}]
            },
            "generationConfig": {
                "temperature": 0.0,
                "maxOutputTokens": 1000
            }
        }

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code != 200:
                raise RuntimeError(f"Gemini API returned status {resp.status_code}: {resp.text}")
            
            data = resp.json()
            candidates = data.get("candidates", [])
            if not candidates:
                raise RuntimeError("No candidate responses returned by LLM.")
            
            return candidates[0]["content"]["parts"][0]["text"].strip()

    async def execute_query(self, user_query: str) -> Dict[str, Any]:
        """Translates natural language to SQL, securely executes on PostgreSQL, and synthesizes answers."""
        # 1. Input Sanitization
        try:
            sanitized_query = self.sanitize_user_input(user_query)
        except ValueError as e:
            return {
                "success": False,
                "sql": None,
                "results": None,
                "answer": str(e),
                "error": str(e)
            }

        # 2. LLM Text-to-SQL Generation
        try:
            raw_sql = await self._call_gemini_api(
                prompt=f"Generate the PostgreSQL SELECT query for: {sanitized_query}",
                system_instruction=POSTGRES_SCHEMA_PROMPT
            )
            validated_sql = self.validate_and_harden_sql(raw_sql)
        except Exception as e:
            logger.error(f"SQL generation/validation error: {e}")
            return {
                "success": False,
                "sql": None,
                "results": None,
                "answer": f"Unable to generate a valid database query: {str(e)}",
                "error": str(e)
            }

        # 3. PostgreSQL Execution in Read-Only Sandbox Transaction
        results = []
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    # Security Guardrails at the PostgreSQL Engine level:
                    await cur.execute("SET LOCAL TRANSACTION READ ONLY;")
                    await cur.execute("SET LOCAL statement_timeout = '3000ms';")
                    
                    await cur.execute(validated_sql)
                    rows = await cur.fetchall()
                    results = [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"Database execution failed for query '{validated_sql}': {e}")
            return {
                "success": False,
                "sql": validated_sql,
                "results": None,
                "answer": f"SQL execution error: {str(e)}",
                "error": str(e)
            }

        # 4. Indirect Prompt Injection Shield: Synthesize User-Friendly Answer
        synthesis_prompt = format_synthesis_user_prompt(
            sanitized_query=sanitized_query,
            validated_sql=validated_sql,
            results_json=json.dumps(results[:15], default=str)
        )

        try:
            answer = await self._call_gemini_api(
                prompt=synthesis_prompt,
                system_instruction=NL_QUERY_SYNTHESIS_SYSTEM_PROMPT
            )
        except Exception as e:
            answer = f"Found {len(results)} matching records."

        return {
            "success": True,
            "sql": validated_sql,
            "results": results,
            "answer": answer,
            "error": None
        }

nl_query_service = NaturalLanguageQueryService()
