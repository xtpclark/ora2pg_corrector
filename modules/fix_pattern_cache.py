"""
Fix Pattern Cache — stores error→fix mappings to avoid redundant AI calls.

When SQL validation fails and AI provides a fix, the error pattern and the
transform (what changed) are cached. On future similar errors, the cached
transform is applied directly — zero AI tokens.

Patterns are normalized: object names, line numbers, and specifics are stripped
so that "relation X already exists" and "relation Y already exists" match the
same pattern.
"""

import hashlib
import json
import logging
import os
import re
import sqlite3
import difflib
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class FixPatternCache:
    """SQLite-backed cache of error→fix patterns for SQL validation."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_table()

    def _ensure_table(self):
        """Create the cache table if it doesn't exist."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fix_patterns (
                    pattern_hash TEXT PRIMARY KEY,
                    error_pattern TEXT NOT NULL,
                    object_type TEXT,
                    fix_description TEXT,
                    search_regex TEXT NOT NULL,
                    replace_template TEXT NOT NULL,
                    hit_count INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    @staticmethod
    def normalize_error(error_message: str) -> str:
        """Normalize an error message into a reusable pattern.

        Strips object-specific names, line/column numbers, and whitespace
        so structurally identical errors produce the same pattern.
        """
        msg = error_message.strip()
        # Strip LINE N: ... context lines
        msg = re.sub(r'\nLINE \d+:.*', '', msg, flags=re.DOTALL)
        # Normalize quoted identifiers to placeholder
        msg = re.sub(r'"[^"]+?"', '"__ID__"', msg)
        # Normalize line/position references
        msg = re.sub(r'line \d+', 'line N', msg, flags=re.IGNORECASE)
        msg = re.sub(r'position \d+', 'position N', msg, flags=re.IGNORECASE)
        # Collapse whitespace
        msg = re.sub(r'\s+', ' ', msg).strip()
        return msg

    @staticmethod
    def _pattern_hash(normalized_error: str, object_type: str = "") -> str:
        """Compute a stable hash for a normalized error + object type."""
        key = f"{object_type}:{normalized_error}"
        return hashlib.sha256(key.encode()).hexdigest()[:16]

    @staticmethod
    def _build_transform(original_sql: str, fixed_sql: str) -> Tuple[str, str]:
        """Build a search→replace transform from an original/fixed SQL pair.

        Returns (search_regex, replace_template) that can be applied to
        future SQL with the same error pattern.
        """
        # Find the differing lines
        orig_lines = original_sql.splitlines(keepends=True)
        fixed_lines = fixed_sql.splitlines(keepends=True)

        opcodes = difflib.SequenceMatcher(None, orig_lines, fixed_lines).get_opcodes()

        search_parts = []
        replace_parts = []

        for tag, i1, i2, j1, j2 in opcodes:
            if tag == 'equal':
                continue
            if tag in ('replace', 'delete'):
                for line in orig_lines[i1:i2]:
                    search_parts.append(line.strip())
            if tag in ('replace', 'insert'):
                for line in fixed_lines[j1:j2]:
                    replace_parts.append(line.strip())

        search = '\n'.join(search_parts)
        replace = '\n'.join(replace_parts)
        return search, replace

    def lookup(self, error_message: str, object_type: str = "") -> Optional[Tuple[str, str]]:
        """Look up a cached fix for a given error.

        Returns (search_text, replace_text) if found, None otherwise.
        """
        normalized = self.normalize_error(error_message)
        h = self._pattern_hash(normalized, object_type)

        try:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT search_regex, replace_template FROM fix_patterns WHERE pattern_hash = ?",
                    (h,)
                ).fetchone()
                if row:
                    conn.execute(
                        "UPDATE fix_patterns SET hit_count = hit_count + 1, "
                        "last_used_at = CURRENT_TIMESTAMP WHERE pattern_hash = ?",
                        (h,)
                    )
                    logger.info(f"Fix pattern cache HIT for: {normalized[:80]}")
                    return row[0], row[1]
        except Exception as e:
            logger.warning(f"Fix pattern cache lookup failed: {e}")

        return None

    def store(self, error_message: str, original_sql: str, fixed_sql: str,
              object_type: str = "", fix_description: str = ""):
        """Store a new error→fix pattern."""
        normalized = self.normalize_error(error_message)
        h = self._pattern_hash(normalized, object_type)
        search, replace = self._build_transform(original_sql, fixed_sql)

        if not search and not replace:
            return  # No meaningful diff to cache

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO fix_patterns
                        (pattern_hash, error_pattern, object_type, fix_description,
                         search_regex, replace_template)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(pattern_hash) DO UPDATE SET
                        hit_count = hit_count + 1,
                        last_used_at = CURRENT_TIMESTAMP
                """, (h, normalized, object_type, fix_description, search, replace))
            logger.info(f"Fix pattern cache STORE: {normalized[:80]}")
        except Exception as e:
            logger.warning(f"Fix pattern cache store failed: {e}")

    def apply_fix(self, sql: str, search_text: str, replace_text: str) -> Optional[str]:
        """Apply a cached fix transform to SQL.

        Returns the fixed SQL if the search text was found, None otherwise.
        """
        if search_text in sql:
            return sql.replace(search_text, replace_text, 1)

        # Try line-by-line flexible matching (whitespace-insensitive)
        search_lines = [l.strip() for l in search_text.splitlines() if l.strip()]
        sql_lines = sql.splitlines()

        for i, sql_line in enumerate(sql_lines):
            if search_lines and sql_line.strip() == search_lines[0]:
                # Check if consecutive lines match
                match = True
                for j, search_line in enumerate(search_lines):
                    if i + j >= len(sql_lines) or sql_lines[i + j].strip() != search_line:
                        match = False
                        break
                if match:
                    # Replace the matched block
                    replace_lines = replace_text.splitlines()
                    new_lines = sql_lines[:i] + replace_lines + sql_lines[i + len(search_lines):]
                    return '\n'.join(new_lines)

        return None

    def stats(self) -> dict:
        """Return cache statistics."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT COUNT(*), COALESCE(SUM(hit_count), 0) FROM fix_patterns"
                ).fetchone()
                return {"patterns": row[0], "total_hits": row[1]}
        except Exception:
            return {"patterns": 0, "total_hits": 0}
