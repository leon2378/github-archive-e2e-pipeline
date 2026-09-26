"""Pull hourly GH Archive files, slim them down, and push them to a Unity Catalog volume.

GH Archive (https://www.gharchive.org) publishes one gzipped NDJSON file per hour containing every
public GitHub event (roughly 80-90k events an hour). Before uploading we keep each event's envelope
(who, what, where, when) and only the scalar fields of its payload, dropping nested objects such as
avatar/URL blocks and PR or issue bodies. That trims about a third of the volume and gives bronze
one uniform shape across all event types.

Runs are idempotent: an hour that already exists in the volume is skipped, so the scheduled run
can use an overlapping look-back window and recovers on its own after a missed day.

Usage:
    python -m ingestion.gharchive                                       # last 48 hours
    python -m ingestion.gharchive --start 2026-09-01T00 --end 2026-09-02T00
    python -m ingestion.gharchive --output-dir data/                    # local only, no Databricks
"""

from __future__ import annotations

import argparse
import gzip
import logging
import os
import tempfile
import time
from collections import Counter
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import orjson

from .sinks import LocalSink, Sink, VolumeSink

log = logging.getLogger("gharchive")

GHARCHIVE_URL = "https://data.gharchive.org/{key}.json.gz"
DEFAULT_VOLUME_PATH = "/Volumes/workspace/gharchive_dev/landing"
SCALAR_TYPES = (str, int, float, bool)


# --- Time helpers -------------------------------------------------------------------------------


def hour_key(hour: datetime) -> str:
    """GH Archive's name for an hour, e.g. ``2026-09-01-7`` (the hour is not zero-padded)."""
    return f"{hour:%Y-%m-%d}-{hour.hour}"


def parse_hour(value: str) -> datetime:
    """Parse an ISO date or timestamp (``2026-09-01T05``) and floor it to a UTC hour."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def hours_between(start: datetime, end: datetime) -> list[datetime]:
    """Every hour in the half-open range ``[start, end)``."""
    hours = []
    hour = start
    while hour < end:
        hours.append(hour)
        hour += timedelta(hours=1)
    return hours


# --- Transform ----------------------------------------------------------------------------------


def slim_event(event: dict) -> dict:
    """Keep the event envelope plus the scalar part of its payload."""
    actor = event.get("actor") or {}
    repo = event.get("repo") or {}
    org = event.get("org")
    payload = event.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    return {
        "id": event.get("id"),
        "type": event.get("type"),
        "created_at": event.get("created_at"),
        "public": event.get("public"),
        "actor": {"id": actor.get("id"), "login": actor.get("login")},
        "repo": {"id": repo.get("id"), "name": repo.get("name")},
        "org": {"id": org.get("id"), "login": org.get("login")} if org else None,
        "payload": {k: v for k, v in payload.items() if isinstance(v, SCALAR_TYPES)},
    }


@dataclass
class SlimStats:
    events_in: int = 0
    events_out: int = 0
    bad_lines: int = 0


def slim_file(src: Path, dst: Path, event_types: frozenset[str] | None = None) -> SlimStats:
    """Stream a raw GH Archive file into a slimmed gzipped NDJSON file."""
    stats = SlimStats()
    with gzip.open(src, "rb") as fin, gzip.open(dst, "wb", compresslevel=6) as fout:
        for line in fin:
            if not line.strip():
                continue
            try:
                event = orjson.loads(line)
            except orjson.JSONDecodeError:
                stats.bad_lines += 1
                continue
            stats.events_in += 1
            if event_types and event.get("type") not in event_types:
                continue
            fout.write(orjson.dumps(slim_event(event), option=orjson.OPT_APPEND_NEWLINE))
            stats.events_out += 1
    return stats


# --- Extract ------------------------------------------------------------------------------------


def download(client: httpx.Client, key: str, dst: Path, attempts: int = 4) -> bool:
    """Download one hour to ``dst``. Returns False if GH Archive doesn't have that hour (yet)."""
    url = GHARCHIVE_URL.format(key=key)
    for attempt in range(1, attempts + 1):
        try:
            with client.stream("GET", url) as response:
                if response.status_code == 404:
                    return False
                response.raise_for_status()
                with dst.open("wb") as f:
                    for chunk in response.iter_bytes(1 << 20):
                        f.write(chunk)
            return True
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            retryable = not isinstance(exc, httpx.HTTPStatusError) or (
                exc.response.status_code >= 500 or exc.response.status_code == 429
            )
            if not retryable or attempt == attempts:
                raise
            wait = 2**attempt
            log.warning("%s: %s - retrying in %ss", key, exc, wait)
            time.sleep(wait)
    raise AssertionError("unreachable")


# --- Orchestration ------------------------------------------------------------------------------


def target_path(hour: datetime) -> str:
    return f"events/{hour:%Y-%m-%d}/{hour_key(hour)}.json.gz"


def ingest_hour(
    hour: datetime,
    sink: Sink,
    client: httpx.Client,
    workdir: Path,
    event_types: frozenset[str] | None,
) -> str:
    key = hour_key(hour)
    rel_path = target_path(hour)
    if sink.exists(rel_path):
        return "skipped"

    raw = workdir / f"{key}.raw.json.gz"
    slim = workdir / f"{key}.json.gz"
    try:
        if not download(client, key, raw):
            log.warning("%s: not published on GH Archive (yet), skipping", key)
            return "missing"
        stats = slim_file(raw, slim, event_types)
        sink.put(slim, rel_path)
        log.info(
            "%s: %d events -> %d kept, %d bad lines, %.1f MB -> %.1f MB",
            key,
            stats.events_in,
            stats.events_out,
            stats.bad_lines,
            raw.stat().st_size / 1e6,
            slim.stat().st_size / 1e6,
        )
        return "uploaded"
    finally:
        raw.unlink(missing_ok=True)
        slim.unlink(missing_ok=True)


def run(
    hours: Iterable[datetime],
    sink: Sink,
    event_types: frozenset[str] | None = None,
    workers: int = 4,
) -> Counter[str]:
    """Ingest the given hours in parallel. Returns a count per outcome."""
    counts: Counter[str] = Counter()
    timeout = httpx.Timeout(120.0, connect=15.0)
    with (
        tempfile.TemporaryDirectory() as tmp,
        httpx.Client(timeout=timeout, follow_redirects=True) as client,
        ThreadPoolExecutor(max_workers=workers) as pool,
    ):
        futures = {
            pool.submit(ingest_hour, hour, sink, client, Path(tmp), event_types): hour
            for hour in hours
        }
        for future in as_completed(futures):
            try:
                counts[future.result()] += 1
            except Exception:
                log.exception("%s: failed", hour_key(futures[future]))
                counts["failed"] += 1
    return counts


def _hour_arg(value: str) -> datetime:
    try:
        return parse_hour(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not an ISO date/hour: {value!r}") from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--start",
        type=_hour_arg,
        default=os.environ.get("INGEST_START") or None,
        help="First hour to ingest, UTC (e.g. 2026-09-01T00). "
        "Default: --end minus --lookback-hours.",
    )
    parser.add_argument(
        "--end",
        type=_hour_arg,
        default=os.environ.get("INGEST_END") or None,
        help="Hour to stop before, UTC (exclusive). Default: the current hour.",
    )
    parser.add_argument("--lookback-hours", type=int, default=48)
    parser.add_argument(
        "--volume-path",
        default=os.environ.get("LANDING_VOLUME_PATH", DEFAULT_VOLUME_PATH),
        help=f"Unity Catalog volume to upload to. Default: {DEFAULT_VOLUME_PATH}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Write to this local directory instead of Databricks.",
    )
    parser.add_argument(
        "--event-types",
        type=lambda s: frozenset(t.strip() for t in s.split(",") if t.strip()),
        help="Comma-separated event types to keep, e.g. WatchEvent,ForkEvent. Default: all.",
    )
    parser.add_argument("--workers", type=int, default=4)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is too noisy

    end = args.end or datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start = args.start or end - timedelta(hours=args.lookback_hours)
    if start >= end:
        raise SystemExit(f"--start ({start:%Y-%m-%dT%H}) must be before --end ({end:%Y-%m-%dT%H})")
    hours = hours_between(start, end)

    sink: Sink = LocalSink(args.output_dir) if args.output_dir else VolumeSink(args.volume_path)
    destination = args.output_dir or args.volume_path
    log.info("Ingesting %d hours [%s, %s) into %s", len(hours), start, end, destination)

    counts = run(hours, sink, args.event_types, args.workers)
    log.info("Done: %s", dict(counts))
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
