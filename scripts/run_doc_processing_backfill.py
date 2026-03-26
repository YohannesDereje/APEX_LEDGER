import asyncio
import json
import os
import sys
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ledger.commands.handlers import (
    StartDocumentProcessingCommand,
    handle_start_document_processing,
)
from ledger.event_store import EventStore


async def _initialize_schema(database_url: str, schema_path: Path, projection_schema_path: Path) -> None:
    db_conn = await asyncpg.connect(database_url)
    try:
        await db_conn.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        with open(schema_path, "r", encoding="utf-8") as schema_file:
            await db_conn.execute(schema_file.read())
        with open(projection_schema_path, "r", encoding="utf-8") as projection_schema_file:
            await db_conn.execute(projection_schema_file.read())
    finally:
        await db_conn.close()

async def main():
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set.")

    schema_path = PROJECT_ROOT / "ledger" / "schema.sql"
    projection_schema_path = PROJECT_ROOT / "ledger" / "projections" / "schema.sql"

    user_pass_host = database_url.split("://")[1].split("@")[0]
    host = database_url.split("@")[1].split("/")[0]
    user = user_pass_host.split(":")[0]
    password = user_pass_host.split(":")[1]

    database_name = database_url.rsplit("/", 1)[-1]
    admin_database_url = f"postgresql://{user}:{password}@{host}/postgres"

    async def init_connection(conn):
        await conn.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
            format="text",
        )

    admin_conn = await asyncpg.connect(admin_database_url)
    try:
        exists = await admin_conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            database_name,
        )
        if not exists:
            await admin_conn.execute(f'CREATE DATABASE "{database_name}"')
            print(f"Database '{database_name}' created. Now creating tables...")
            await _initialize_schema(database_url, schema_path, projection_schema_path)
    finally:
        await admin_conn.close()

    schema_check_conn = await asyncpg.connect(database_url)
    try:
        core_table_exists = await schema_check_conn.fetchval(
            "SELECT to_regclass('public.event_streams')"
        )
        projection_table_exists = await schema_check_conn.fetchval(
            "SELECT to_regclass('public.application_summary')"
        )
    finally:
        await schema_check_conn.close()

    if not core_table_exists or not projection_table_exists:
        print(f"Database '{database_name}' is missing required tables. Initializing schema...")
        await _initialize_schema(database_url, schema_path, projection_schema_path)

    pool = await asyncpg.create_pool(database_url, init=init_connection)
    event_store = EventStore(pool)

    try:
        script_dir = Path(__file__).resolve().parent
        repo_root = script_dir.parent
        documents_dir = repo_root / "documents"
        data_dir = repo_root / "data"

        if not documents_dir.exists():
            raise RuntimeError(f"Documents directory not found: {documents_dir}")

        if not data_dir.exists():
            print(f"Warning: data directory not found: {data_dir}")

        application_ids = sorted(
            path.name
            for path in documents_dir.iterdir()
            if path.is_dir()
        )

        for application_id in application_ids:
            print(f"Processing application: {application_id}")
            application_documents_dir = documents_dir / application_id

            document_files = sorted(
                path
                for path in application_documents_dir.rglob("*")
                if path.is_file() and path.suffix.lower() in {".pdf", ".png", ".jpg", ".jpeg", ".tiff"}
            )

            documents = [
                {
                    "document_id": file_path.stem,
                    "pdf_path": str(file_path),
                    "filename": file_path.name,
                    "document_type": "INCOME_STATEMENT",
                    "content": "",
                }
                for file_path in document_files
            ]

            cmd = StartDocumentProcessingCommand(
                application_id=application_id,
                documents=documents,
            )
            await handle_start_document_processing(cmd, event_store)
    finally:
        await event_store._pool.close()


if __name__ == "__main__":
    asyncio.run(main())