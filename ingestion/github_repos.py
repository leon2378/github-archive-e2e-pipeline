"""Enrich repos with metadata from the GitHub REST API and land it in a Unity Catalog volume.

Which repos to fetch is decided in dbt (the `ops_repo_enrichment_queue` model). This job works
through that queue: it reads the top rows, fetches each repo, and writes one gzipped NDJSON file
per run. Every attempt is recorded, including repos that no longer exist, so downstream models
can tell "not fetched yet" apart from "deleted".

- Conditional requests: when the queue has a repo's ETag, the request sends If-None-Match. A 304
  reply means nothing changed, and GitHub doesn't count it against the rate limit.
- Renamed repos redirect to their new name. Deleted or private repos (404) and blocked repos
  (403/451) are recorded with their status.
- The job watches `x-ratelimit-remaining` and stops early when quota runs low, leaving the rest
  of the queue for the next run.

Usage:
    python -m ingestion.github_repos                              # queue from Databricks
    python -m ingestion.github_repos --max-requests 50
    python -m ingestion.github_repos --queue-file q.jsonl --output-dir data   # fully local
"""

from __future__ import annotations

import argparse
import gzip
import logging
import os
import re
import tempfile
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
import orjson

from .sinks import LocalSink, Sink, VolumeSink

log = logging.getLogger("github_repos")

API_URL = "https://api.github.com"
DEFAULT_VOLUME_PATH = "/Volumes/workspace/gharchive_dev/landing"
DEFAULT_QUEUE_TABLE = "workspace.gharchive_dev_gold.ops_repo_enrichment_queue"
DEFAULT_WAREHOUSE = "Serverless Starter Warehouse"
STATUS_NAMES = {200: "ok", 304: "not_modified", 403: "blocked", 404: "not_found", 451: "blocked"}


@dataclass(frozen=True)
class QueueItem:
    repo_id: int
    repo_name: str
    etag: str | None = None


class RateLimited(Exception):
    """GitHub says we're out of quota (primary or secondary rate limit)."""


# --- Queue --------------------------------------------------------------------------------------


def read_queue_file(path: Path, limit: int) -> list[QueueItem]:
    """Read a queue from a JSON-lines file with repo_id, repo_name and optional etag."""
    items = []
    with path.open("rb") as f:
        for line in f:
            if line.strip():
                row = orjson.loads(line)
                items.append(QueueItem(int(row["repo_id"]), row["repo_name"], row.get("etag")))
    return items[:limit]


def read_queue_table(table: str, limit: int, warehouse: str) -> list[QueueItem] | None:
    """Read the top of the queue table through a SQL warehouse. None if the table doesn't exist."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    if not re.fullmatch(r"[\w`]+(\.[\w`]+){0,2}", table):
        raise SystemExit(f"Not a valid table name: {table!r}")

    w = WorkspaceClient()
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID") or next(
        (wh.id for wh in w.warehouses.list() if wh.name == warehouse), None
    )
    if warehouse_id is None:
        raise SystemExit(f"SQL warehouse {warehouse!r} not found")

    sql = f"select repo_id, repo_name, etag from {table} order by priority limit {int(limit)}"
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="50s"
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        message = resp.status.error.message if resp.status.error else str(resp.status.state)
        if "TABLE_OR_VIEW_NOT_FOUND" in message:
            return None
        raise RuntimeError(f"Queue query failed: {message}")
    rows = (resp.result and resp.result.data_array) or []
    return [QueueItem(int(repo_id), name, etag or None) for repo_id, name, etag in rows]


# --- Transform ----------------------------------------------------------------------------------


def slim_repo(repo: dict) -> dict:
    """Keep the repo attributes and metrics the models use; drop URLs and nested API objects."""
    owner = repo.get("owner") or {}
    license_ = repo.get("license") or {}
    parent = repo.get("parent") or {}
    return {
        "id": repo.get("id"),
        "full_name": repo.get("full_name"),
        "owner_login": owner.get("login"),
        "owner_type": owner.get("type"),
        "description": repo.get("description"),
        "homepage": repo.get("homepage") or None,
        "language": repo.get("language"),
        "topics": repo.get("topics") or [],
        "license_spdx_id": license_.get("spdx_id"),
        "is_fork": repo.get("fork"),
        "parent_full_name": parent.get("full_name"),
        "is_archived": repo.get("archived"),
        "is_disabled": repo.get("disabled"),
        "is_template": repo.get("is_template"),
        "visibility": repo.get("visibility"),
        "default_branch": repo.get("default_branch"),
        "size_kb": repo.get("size"),
        "stargazers_count": repo.get("stargazers_count"),
        "forks_count": repo.get("forks_count"),
        "open_issues_count": repo.get("open_issues_count"),
        "subscribers_count": repo.get("subscribers_count"),
        "created_at": repo.get("created_at"),
        "updated_at": repo.get("updated_at"),
        "pushed_at": repo.get("pushed_at"),
    }


# --- Extract ------------------------------------------------------------------------------------


def make_client(token: str | None, transport: httpx.BaseTransport | None = None) -> httpx.Client:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "github-archive-e2e-pipeline",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.Client(
        base_url=API_URL,
        headers=headers,
        timeout=httpx.Timeout(30.0, connect=10.0),
        follow_redirects=True,  # renamed repos answer with a 301 to their new location
        transport=transport,
    )


def _is_rate_limited(resp: httpx.Response) -> bool:
    # A 403 can also mean "this repo is blocked", so only treat it as a rate limit when GitHub
    # says so: quota exhausted, a retry-after header, or a secondary-limit message in the body.
    return resp.status_code in (403, 429) and (
        resp.headers.get("retry-after") is not None
        or resp.headers.get("x-ratelimit-remaining") == "0"
        or "rate limit" in resp.text.lower()
    )


def fetch_repo(client: httpx.Client, item: QueueItem, attempts: int = 3) -> httpx.Response:
    """GET one repo, retrying transient failures. Raises RateLimited when out of quota."""
    headers = {"If-None-Match": item.etag} if item.etag else {}
    for attempt in range(1, attempts + 1):
        try:
            resp = client.get(f"/repos/{item.repo_name}", headers=headers)
        except httpx.TransportError as exc:
            if attempt == attempts:
                raise
            log.warning("%s: %s - retrying", item.repo_name, exc)
            time.sleep(2**attempt)
            continue
        if _is_rate_limited(resp):
            retry_after = int(resp.headers.get("retry-after", "0") or 0)
            if 0 < retry_after <= 60 and attempt < attempts:
                log.warning("Secondary rate limit, waiting %ss", retry_after)
                time.sleep(retry_after)
                continue
            raise RateLimited(f"rate limited (retry-after={retry_after or 'n/a'})")
        if resp.status_code >= 500 and attempt < attempts:
            log.warning("%s: HTTP %s - retrying", item.repo_name, resp.status_code)
            time.sleep(2**attempt)
            continue
        return resp
    raise AssertionError("unreachable")


def to_record(item: QueueItem, resp: httpx.Response, fetched_at: str) -> dict:
    return {
        "repo_id": item.repo_id,
        "requested_name": item.repo_name,
        "fetched_at": fetched_at,
        "http_status": resp.status_code,
        "etag": resp.headers.get("etag") or item.etag,
        "repo": slim_repo(resp.json()) if resp.status_code == 200 else None,
    }


def run(
    items: Iterable[QueueItem],
    client: httpx.Client,
    max_requests: int = 900,
    reserve: int = 25,
) -> tuple[list[dict], Counter[str]]:
    """Fetch repos one at a time (GitHub asks for serial requests) until the queue or quota ends.

    `max_requests` caps quota-consuming calls; 304s are free and don't count toward it.
    """
    records: list[dict] = []
    counts: Counter[str] = Counter()
    spent = 0
    for item in items:
        if spent >= max_requests:
            counts["deferred_cap"] += 1
            continue
        try:
            resp = fetch_repo(client, item)
        except RateLimited as exc:
            log.warning("Stopping: %s. The rest of the queue waits for the next run.", exc)
            counts["stopped_rate_limited"] += 1
            break
        except httpx.HTTPError as exc:
            log.error("%s: %s", item.repo_name, exc)
            counts["error"] += 1
            continue

        fetched_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        status = STATUS_NAMES.get(resp.status_code, "error")
        counts[status] += 1
        if resp.status_code != 304:
            spent += 1
        if status == "error":
            log.error("%s: unexpected HTTP %s", item.repo_name, resp.status_code)
            continue
        records.append(to_record(item, resp, fetched_at))

        remaining = resp.headers.get("x-ratelimit-remaining")
        if remaining is not None and int(remaining) <= reserve:
            log.warning("Only %s requests left this hour; stopping early.", remaining)
            counts["stopped_low_quota"] += 1
            break
    return records, counts


# --- Load ---------------------------------------------------------------------------------------


def write_ndjson_gz(records: list[dict], path: Path) -> None:
    with gzip.open(path, "wb", compresslevel=6) as f:
        for record in records:
            f.write(orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE))


def target_path(now: datetime) -> str:
    return f"repos/{now:%Y-%m-%d}/repos-{now:%Y%m%dT%H%M%SZ}.json.gz"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--queue-table",
        default=os.environ.get("ENRICHMENT_QUEUE_TABLE", DEFAULT_QUEUE_TABLE),
        help=f"dbt queue model to read. Default: {DEFAULT_QUEUE_TABLE}",
    )
    parser.add_argument("--queue-file", type=Path, help="Read the queue from a JSON-lines file.")
    parser.add_argument("--warehouse", default=DEFAULT_WAREHOUSE, help="SQL warehouse name.")
    parser.add_argument("--limit", type=int, default=1500, help="Max queue rows to read.")
    parser.add_argument(
        "--max-requests", type=int, default=900, help="Max quota-consuming API calls this run."
    )
    parser.add_argument(
        "--volume-path",
        default=os.environ.get("LANDING_VOLUME_PATH", DEFAULT_VOLUME_PATH),
        help=f"Unity Catalog volume to upload to. Default: {DEFAULT_VOLUME_PATH}",
    )
    parser.add_argument("--output-dir", type=Path, help="Write locally instead of Databricks.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even if enrichment already landed a file today (UTC).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    # The workflow has a backup schedule in case GitHub drops the first run, so enrichment runs
    # at most once per UTC day: a second run would only spend API quota on the same repos.
    sink: Sink = LocalSink(args.output_dir) if args.output_dir else VolumeSink(args.volume_path)
    today = f"repos/{datetime.now(UTC):%Y-%m-%d}"
    if not args.force and sink.has_files(today):
        log.info("Enrichment already ran today (%s has files); skipping. Use --force.", today)
        return 0

    if args.queue_file:
        items = read_queue_file(args.queue_file, args.limit)
    else:
        items = read_queue_table(args.queue_table, args.limit, args.warehouse)
        if items is None:
            log.warning(
                "Queue %s doesn't exist yet (built by the daily dbt run).", args.queue_table
            )
            return 0
    if not items:
        log.info("Queue is empty; nothing to do.")
        return 0

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        log.warning("GITHUB_TOKEN not set: unauthenticated requests are limited to 60/hour.")

    log.info("Enriching up to %d of %d queued repos", args.max_requests, len(items))
    with make_client(token) as client:
        records, counts = run(items, client, max_requests=args.max_requests)
    log.info("Results: %s", dict(counts))

    if records:
        rel_path = target_path(datetime.now(UTC))
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / "repos.json.gz"
            write_ndjson_gz(records, local)
            sink.put(local, rel_path)
        log.info("Landed %d records at %s", len(records), rel_path)

    attempted = sum(counts[s] for s in ("ok", "not_modified", "not_found", "blocked", "error"))
    return 1 if attempted and counts["error"] > attempted / 2 else 0


if __name__ == "__main__":
    raise SystemExit(main())
