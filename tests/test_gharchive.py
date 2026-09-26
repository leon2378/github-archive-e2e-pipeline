import gzip
from datetime import UTC, datetime
from pathlib import Path

import orjson
import pytest

from ingestion import gharchive


def make_event(**overrides) -> dict:
    event = {
        "id": "1",
        "type": "WatchEvent",
        "created_at": "2026-09-01T05:03:07Z",
        "public": True,
        "actor": {"id": 7, "login": "octocat", "avatar_url": "https://avatars.example/7"},
        "repo": {"id": 42, "name": "octo/hello", "url": "https://api.github.com/repos/octo/hello"},
        "org": {"id": 9, "login": "octo", "avatar_url": "https://avatars.example/9"},
        "payload": {"action": "started"},
    }
    return event | overrides


def write_gz(path: Path, lines: list[bytes]) -> None:
    with gzip.open(path, "wb") as f:
        f.write(b"\n".join(lines) + b"\n")


def read_gz(path: Path) -> list[dict]:
    with gzip.open(path, "rb") as f:
        return [orjson.loads(line) for line in f]


def test_hour_key_is_not_zero_padded():
    assert gharchive.hour_key(datetime(2026, 9, 1, 7, tzinfo=UTC)) == "2026-09-01-7"
    assert gharchive.hour_key(datetime(2026, 9, 1, 17, tzinfo=UTC)) == "2026-09-01-17"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-09-01T05", datetime(2026, 9, 1, 5, tzinfo=UTC)),
        ("2026-09-01", datetime(2026, 9, 1, 0, tzinfo=UTC)),
        ("2026-09-01T05:59:59", datetime(2026, 9, 1, 5, tzinfo=UTC)),
        ("2026-09-01T05:30:00+02:00", datetime(2026, 9, 1, 3, tzinfo=UTC)),
    ],
)
def test_parse_hour_floors_to_a_utc_hour(value, expected):
    assert gharchive.parse_hour(value) == expected


def test_hours_between_is_half_open_and_crosses_days():
    hours = gharchive.hours_between(
        datetime(2026, 9, 1, 22, tzinfo=UTC), datetime(2026, 9, 2, 1, tzinfo=UTC)
    )
    assert [gharchive.hour_key(h) for h in hours] == [
        "2026-09-01-22",
        "2026-09-01-23",
        "2026-09-02-0",
    ]


def test_slim_event_keeps_envelope_and_scalar_payload_fields():
    raw = make_event(
        type="PullRequestEvent",
        payload={
            "action": "opened",
            "number": 12,
            "pull_request": {"title": "a large nested object"},
            "labels": ["bug"],
            "assignee": None,
        },
    )
    assert gharchive.slim_event(raw) == {
        "id": "1",
        "type": "PullRequestEvent",
        "created_at": "2026-09-01T05:03:07Z",
        "public": True,
        "actor": {"id": 7, "login": "octocat"},
        "repo": {"id": 42, "name": "octo/hello"},
        "org": {"id": 9, "login": "octo"},
        "payload": {"action": "opened", "number": 12},
    }


def test_slim_event_without_org_or_payload():
    raw = make_event(payload=None)
    del raw["org"]
    slim = gharchive.slim_event(raw)
    assert slim["org"] is None
    assert slim["payload"] == {}


def test_slim_file_filters_event_types_and_skips_bad_lines(tmp_path):
    src, dst = tmp_path / "raw.json.gz", tmp_path / "slim.json.gz"
    write_gz(
        src,
        [
            orjson.dumps(make_event(id="1")),
            b"{not valid json",
            orjson.dumps(make_event(id="2", type="PushEvent")),
        ],
    )

    stats = gharchive.slim_file(src, dst, event_types=frozenset({"WatchEvent"}))

    assert stats == gharchive.SlimStats(events_in=2, events_out=1, bad_lines=1)
    assert [e["id"] for e in read_gz(dst)] == ["1"]


def test_run_uploads_new_hours_and_skips_existing_and_unpublished(tmp_path, monkeypatch):
    volume = tmp_path / "volume"
    hours = gharchive.hours_between(
        datetime(2026, 9, 1, 0, tzinfo=UTC), datetime(2026, 9, 1, 3, tzinfo=UTC)
    )
    # Hour 0 was loaded by an earlier run; hour 2 isn't published on GH Archive yet.
    existing = volume / "events/2026-09-01/2026-09-01-0.json.gz"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"loaded earlier")

    def fake_download(client, key, dst):
        if key == "2026-09-01-2":
            return False
        write_gz(dst, [orjson.dumps(make_event(id=key))])
        return True

    monkeypatch.setattr(gharchive, "download", fake_download)

    counts = gharchive.run(hours, gharchive.LocalSink(volume), workers=2)

    assert counts == {"skipped": 1, "uploaded": 1, "missing": 1}
    assert existing.read_bytes() == b"loaded earlier"
    uploaded = read_gz(volume / "events/2026-09-01/2026-09-01-1.json.gz")
    assert [e["id"] for e in uploaded] == ["2026-09-01-1"]
