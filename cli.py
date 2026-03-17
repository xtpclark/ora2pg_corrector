#!/usr/bin/env python3
"""
CLI for the Ora2Pg autonomous migration agent.

Runs a full Oracle-to-PostgreSQL migration from the command line,
without requiring Flask, Docker, or the web UI.

Usage:
    python cli.py migrate \\
        --oracle-dsn "host:1521/SID" \\
        --oracle-user ADEMPIERE \\
        --oracle-pwd secret \\
        --schema ADEMPIERE \\
        --pg "postgresql://user:pass@host:5432/targetdb" \\
        --ai-provider anthropic \\
        --ai-model claude-sonnet-4-20250514 \\
        --ai-key sk-ant-...

    # Minimal (no AI — Ora2Pg only, functions/procedures will be exported but not converted):
    python cli.py migrate \\
        --oracle-dsn "host:1521/SID" \\
        --oracle-user ADEMPIERE \\
        --oracle-pwd secret \\
        --schema ADEMPIERE \\
        --pg "postgresql://user:pass@host:5432/targetdb"
"""

import argparse
import logging
import os
import sys
import time

# ---------------------------------------------------------------------------
# Environment setup — must happen before importing app modules so that
# constants.py picks up paths that make sense outside of Docker.
# ---------------------------------------------------------------------------
_data_dir = os.environ.setdefault('APP_DATA_DIR', os.path.join(os.getcwd(), 'data'))
_output_dir = os.environ.setdefault('OUTPUT_DIR', os.path.join(os.getcwd(), 'output'))
_project_data_dir = os.environ.setdefault('PROJECT_DATA_DIR', os.path.join(os.getcwd(), 'project_data'))
os.makedirs(_data_dir, exist_ok=True)
os.makedirs(_output_dir, exist_ok=True)
os.makedirs(_project_data_dir, exist_ok=True)

from modules.db import (
    get_db_standalone, init_db, ENCRYPTION_KEY,
)
from modules.config import load_ora2pg_config
from modules.migration_agent import MigrationAgent, MigrationResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Progress display
# ---------------------------------------------------------------------------

_PHASE_ICONS = {
    'connect':  '🔌',
    'discover': '🔍',
    'assess':   '📋',
    'export':   '📦',
    'convert':  '🔄',
    'validate': '✅',
    'data':     '💾',
    'verify':   '🔎',
    'complete': '🏁',
    'failed':   '❌',
}


def _cli_progress(phase: str, message: str, pct: int):
    """Print progress updates to stdout."""
    icon = _PHASE_ICONS.get(phase, '▶')
    bar_width = 30
    filled = int(bar_width * pct / 100)
    bar = '█' * filled + '░' * (bar_width - filled)
    sys.stdout.write(f"\r{icon} [{bar}] {pct:3d}%  {message:<80}")
    sys.stdout.flush()
    if phase in ('complete', 'failed'):
        sys.stdout.write('\n')


# ---------------------------------------------------------------------------
# Database bootstrap — minimal setup for the agent to work
# ---------------------------------------------------------------------------

def _bootstrap_db() -> None:
    """Initialize the app database and seed Ora2Pg config options.

    The agent needs:
    - migration_sessions table (for export session tracking)
    - migration_files table
    - ora2pg_config_options table (for TYPE list lookup)
    - clients / configs tables (for session FK)
    """
    conn = get_db_standalone()
    if not conn:
        print("ERROR: Could not create application database.", file=sys.stderr)
        sys.exit(1)
    try:
        init_db(conn)
        load_ora2pg_config(conn)
        conn.commit()
    finally:
        conn.close()


def _ensure_cli_client(config: dict) -> int:
    """Create or reuse a 'CLI' client in the app database.

    Returns the client_id.
    """
    from modules.db import execute_query, insert_returning_id
    conn = get_db_standalone()
    try:
        cursor = execute_query(conn, "SELECT client_id FROM clients WHERE client_name = ?", ('CLI',))
        row = cursor.fetchone()
        if row:
            client_id = row['client_id']
        else:
            client_id = insert_returning_id(
                conn, 'clients', ('client_name',), ('CLI',), 'client_id'
            )
            conn.commit()
        return client_id
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='ora2pg-migrate',
        description='Autonomous Oracle-to-PostgreSQL migration agent',
    )
    sub = parser.add_subparsers(dest='command')

    migrate = sub.add_parser('migrate', help='Run a full migration')

    # Oracle connection
    ora = migrate.add_argument_group('Oracle connection')
    ora.add_argument('--oracle-dsn', required=True,
                     help='Oracle DSN (host:port/service_name)')
    ora.add_argument('--oracle-user', required=True,
                     help='Oracle username')
    ora.add_argument('--oracle-pwd', required=True,
                     help='Oracle password')
    ora.add_argument('--schema', required=True,
                     help='Oracle schema to migrate')

    # PostgreSQL target
    pg = migrate.add_argument_group('PostgreSQL target')
    pg.add_argument('--pg', required=True,
                    help='PostgreSQL DSN (postgresql://user:pass@host:port/db)')

    # AI settings (optional)
    ai = migrate.add_argument_group('AI settings (optional — omit for Ora2Pg-only migration)')
    ai.add_argument('--ai-provider', default=None,
                    help='AI provider (anthropic, openai, google, xai)')
    ai.add_argument('--ai-model', default=None,
                    help='AI model ID')
    ai.add_argument('--ai-key', default=None,
                    help='AI API key')
    ai.add_argument('--ai-endpoint', default=None,
                    help='AI endpoint URL')
    ai.add_argument('--ai-temperature', type=float, default=0.2,
                    help='AI temperature (default: 0.2)')
    ai.add_argument('--ai-max-tokens', type=int, default=8192,
                    help='AI max output tokens (default: 8192)')

    # Options
    opts = migrate.add_argument_group('Options')
    opts.add_argument('--ddl-only', action='store_true',
                      help='Skip data migration (DDL/schema only)')
    opts.add_argument('--skip-verify', action='store_true',
                      help='Skip row count verification phase')
    opts.add_argument('-v', '--verbose', action='store_true',
                      help='Verbose logging')
    opts.add_argument('-q', '--quiet', action='store_true',
                      help='Suppress progress bar, print summary only')

    # Status command
    sub.add_parser('status', help='Show fix pattern cache stats')

    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_migrate(args) -> MigrationResult:
    """Build config from CLI args and run the migration agent."""

    # Bootstrap database
    _bootstrap_db()
    client_id = _ensure_cli_client({})

    # Build client_config (same shape as web UI produces)
    client_config = {
        'oracle_dsn': args.oracle_dsn,
        'oracle_user': args.oracle_user,
        'oracle_pwd': args.oracle_pwd,
        'schema': args.schema,
        'oracle_schema': args.schema,
        'validation_pg_dsn': args.pg,
    }

    # Build ai_settings
    ai_settings = {
        'ai_provider': args.ai_provider,
        'ai_endpoint': args.ai_endpoint,
        'ai_model': args.ai_model,
        'ai_api_key': args.ai_key,
        'ai_temperature': args.ai_temperature,
        'ai_max_output_tokens': args.ai_max_tokens,
        'ai_user': 'cli',
        'ai_user_header': '',
        'ssl_cert_path': '',
        'ai_ssl_verify': True,
    }

    if not args.ai_provider:
        print("⚠  No AI provider configured. Functions/procedures will be exported but not converted.")
        print("   Add --ai-provider, --ai-model, --ai-key for AI-powered conversion.\n")

    # Get a DB connection for the agent
    db_conn = get_db_standalone()
    if not db_conn:
        print("ERROR: Could not connect to application database.", file=sys.stderr)
        sys.exit(1)

    callback = None if args.quiet else _cli_progress

    try:
        agent = MigrationAgent(
            client_id=client_id,
            client_config=client_config,
            app_db_conn=db_conn,
            encryption_key=ENCRYPTION_KEY,
            ai_settings=ai_settings,
            callback=callback,
            ddl_only=args.ddl_only,
            skip_verify=args.skip_verify,
        )

        print(f"Starting migration: {args.schema} → {args.pg}")
        print(f"Oracle: {args.oracle_user}@{args.oracle_dsn}\n")

        result = agent.run()

    finally:
        db_conn.close()

    return result


def print_summary(result: MigrationResult):
    """Print a human-readable migration summary."""
    duration = result.duration_seconds
    mins = int(duration // 60)
    secs = int(duration % 60)

    status = "SUCCESS" if result.success else "FAILED"
    print(f"\n{'=' * 60}")
    print(f"  Migration {status}  ({mins}m {secs}s)")
    print(f"{'=' * 60}")
    print(f"  Total objects:    {result.total_objects}")
    print(f"  Migrated:         {result.migrated}")
    print(f"  Failed:           {result.failed}")
    print(f"  Skipped:          {result.skipped}")

    if result.data_tables_migrated:
        print(f"  Tables with data: {result.data_tables_migrated}")
        print(f"  Rows migrated:    {result.data_rows_migrated:,}")

    if result.ai_cost_usd > 0:
        print(f"  AI cost:          ${result.ai_cost_usd:.4f}")

    if result.errors:
        print(f"\n  Errors:")
        for err in result.errors:
            print(f"    - {err}")

    if result.edge_cases:
        print(f"\n  Edge cases ({len(result.edge_cases)}):")
        for ec in result.edge_cases[:20]:  # Cap at 20
            print(f"    - {ec}")
        if len(result.edge_cases) > 20:
            print(f"    ... and {len(result.edge_cases) - 20} more")

    print()


def show_cache_stats():
    """Show fix pattern cache statistics."""
    from modules.fix_pattern_cache import FixPatternCache
    cache_db = os.path.join(_data_dir, 'fix_pattern_cache.db')
    cache = FixPatternCache(cache_db)
    stats = cache.stats()
    print(f"Fix Pattern Cache: {cache_db}")
    print(f"  Patterns stored: {stats['patterns']}")
    print(f"  Total hits:      {stats['total_hits']}")

    if stats['patterns'] > 0:
        import sqlite3
        with sqlite3.connect(cache_db) as conn:
            print(f"\n  Top patterns:")
            for row in conn.execute(
                "SELECT error_pattern, hit_count FROM fix_patterns ORDER BY hit_count DESC LIMIT 10"
            ):
                print(f"    hits={row[1]:4d}  {row[0][:70]}")


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Logging setup
    log_level = logging.DEBUG if getattr(args, 'verbose', False) else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)-8s %(name)s: %(message)s',
    )

    if args.command == 'migrate':
        result = run_migrate(args)
        print_summary(result)
        sys.exit(0 if result.success else 1)

    elif args.command == 'status':
        show_cache_stats()


if __name__ == '__main__':
    main()
