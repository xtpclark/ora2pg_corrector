"""
DDL Parser Module

Parses combined SQL files (from Ora2Pg export) into individual DDL statements.
Tracks object names, types, and line positions for granular migration tracking.
"""

import re
import logging

logger = logging.getLogger(__name__)

# Patterns to identify DDL statement starts
DDL_PATTERNS = {
    'TABLE': re.compile(
        r'^CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["\']?(\w+)["\']?',
        re.IGNORECASE | re.MULTILINE
    ),
    'VIEW': re.compile(
        r'^CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+["\']?(\w+)["\']?',
        re.IGNORECASE | re.MULTILINE
    ),
    'INDEX': re.compile(
        r'^CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?["\']?(\w+)["\']?',
        re.IGNORECASE | re.MULTILINE
    ),
    'SEQUENCE': re.compile(
        r'^CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?["\']?(\w+)["\']?',
        re.IGNORECASE | re.MULTILINE
    ),
    'FUNCTION': re.compile(
        r'^CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+["\']?(\w+)["\']?',
        re.IGNORECASE | re.MULTILINE
    ),
    'PROCEDURE': re.compile(
        r'^CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+["\']?(\w+)["\']?',
        re.IGNORECASE | re.MULTILINE
    ),
    'TRIGGER': re.compile(
        r'^CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+["\']?(\w+)["\']?',
        re.IGNORECASE | re.MULTILINE
    ),
    'TYPE': re.compile(
        r'^CREATE\s+TYPE\s+["\']?(\w+)["\']?',
        re.IGNORECASE | re.MULTILINE
    ),
}

# Pattern to detect any CREATE statement start
ANY_CREATE_PATTERN = re.compile(
    r'^CREATE\s+(?:OR\s+REPLACE\s+)?(?:UNLOGGED\s+)?(?:UNIQUE\s+)?(?:MATERIALIZED\s+)?'
    r'(TABLE|VIEW|INDEX|SEQUENCE|FUNCTION|PROCEDURE|TRIGGER|TYPE)\s+',
    re.IGNORECASE | re.MULTILINE
)

# Patterns for statement terminators
# For functions/procedures, look for $$ or language block end
FUNCTION_END_PATTERN = re.compile(r'\$\$\s*;?\s*$|\bLANGUAGE\s+\w+\s*;', re.IGNORECASE)


def parse_ddl_file(content, object_type_hint=None):
    """
    Parse a DDL file and extract individual objects.

    Args:
        content: The SQL file content as a string
        object_type_hint: Optional hint about the expected object type (TABLE, VIEW, etc.)

    Returns:
        List of dicts with keys:
            - object_name: Name of the database object
            - object_type: Type (TABLE, VIEW, INDEX, etc.)
            - ddl: The complete CREATE statement
            - line_start: Starting line number (1-indexed)
            - line_end: Ending line number (1-indexed)
    """
    objects = []
    lines = content.split('\n')

    # Track current position
    current_object = None
    current_start_line = 0
    current_ddl_lines = []
    in_function_body = False
    paren_depth = 0

    for line_num, line in enumerate(lines, start=1):
        stripped = line.strip()

        # Skip empty lines and comments when not in an object
        if not current_object:
            if not stripped or stripped.startswith('--') or stripped.startswith('SET '):
                continue

            # Check for CREATE statement
            for obj_type, pattern in DDL_PATTERNS.items():
                match = pattern.match(stripped)
                if match:
                    current_object = {
                        'object_name': match.group(1).lower(),
                        'object_type': obj_type,
                        'line_start': line_num,
                    }
                    current_ddl_lines = [line]
                    paren_depth = line.count('(') - line.count(')')

                    # Check if it's a function/procedure (need special termination)
                    in_function_body = obj_type in ('FUNCTION', 'PROCEDURE', 'TRIGGER')

                    # Check if statement completes on same line
                    if not in_function_body and paren_depth <= 0 and stripped.endswith(';'):
                        _save_object(current_object, current_ddl_lines, line_num, objects)
                        current_object = None
                        current_ddl_lines = []
                    break
        else:
            # Continue accumulating current object
            current_ddl_lines.append(line)
            paren_depth += line.count('(') - line.count(')')

            # Determine if statement is complete
            is_complete = False

            if in_function_body:
                # Functions end with $$ followed by optional LANGUAGE clause and ;
                combined = '\n'.join(current_ddl_lines)
                # Count $$ occurrences - function body is between two $$
                dollar_count = combined.count('$$')
                if dollar_count >= 2 and stripped.endswith(';'):
                    is_complete = True
                elif dollar_count >= 2 and FUNCTION_END_PATTERN.search(stripped):
                    is_complete = True
            else:
                # Regular statements end with ; when parentheses are balanced
                if paren_depth <= 0 and stripped.endswith(';'):
                    is_complete = True

            if is_complete:
                _save_object(current_object, current_ddl_lines, line_num, objects)
                current_object = None
                current_ddl_lines = []
                in_function_body = False
                paren_depth = 0

    # Handle unclosed object at end of file
    if current_object and current_ddl_lines:
        _save_object(current_object, current_ddl_lines, len(lines), objects)

    logger.info(f"Parsed {len(objects)} objects from DDL file")
    return objects


def _save_object(obj_info, ddl_lines, end_line, objects_list):
    """Helper to save a parsed object."""
    obj_info['line_end'] = end_line
    obj_info['ddl'] = '\n'.join(ddl_lines)
    objects_list.append(obj_info)
    logger.debug(f"Parsed {obj_info['object_type']} {obj_info['object_name']} "
                 f"(lines {obj_info['line_start']}-{end_line})")


def extract_object_names(content, object_type=None):
    """
    Quick extraction of just object names from DDL content.

    Args:
        content: SQL content
        object_type: Optional filter for specific type

    Returns:
        List of (object_type, object_name) tuples
    """
    results = []

    patterns_to_check = DDL_PATTERNS.items()
    if object_type:
        patterns_to_check = [(object_type, DDL_PATTERNS.get(object_type))]
        if not patterns_to_check[0][1]:
            return results

    for obj_type, pattern in patterns_to_check:
        for match in pattern.finditer(content):
            results.append((obj_type, match.group(1).lower()))

    return results


def count_objects_by_type(content):
    """
    Count objects in DDL content grouped by type.

    Returns:
        Dict mapping object_type to count
    """
    counts = {}
    for obj_type, obj_name in extract_object_names(content):
        counts[obj_type] = counts.get(obj_type, 0) + 1
    return counts


def split_by_object(content):
    """
    Split DDL content into separate strings per object.

    Returns:
        Dict mapping (object_type, object_name) to DDL string
    """
    objects = parse_ddl_file(content)
    return {
        (obj['object_type'], obj['object_name']): obj['ddl']
        for obj in objects
    }
