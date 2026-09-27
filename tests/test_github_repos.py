import gzip
from datetime import UTC, datetime
from pathlib import Path

import httpx
import orjson
import pytest

from ingestion import github_repos as gr
from ingestion.github_repos import QueueItem


def repo_json(repo_id=1, full_name="octo/hello", **overrides) -> dict:
    repo = {
        "id": repo_id,
        "full_name": full_name,
        "owner": {"login": full_name.split("/")[0], "type": "Organization", "avatar_url": "x"},
        "description": "Says hello",
        "homepage": "",
        "language": "Rust",
        "topics": ["cli", "rust"],
        "license": {"key": "mit", "spdx_id": "MIT", "url": "x"},
        "fork": False,
        "archived": False,
        "disabled": False,
        "is_template": False,
        "visibility": "public",
        "default_branch": "main",
        "size": 120,
        "stargazers_count": 42,
        "forks_count": 3,
        "open_issues_count": 1,
        "subscribers_count": 5,
        "created_at": "2026-09-01T00:00:00Z",
        "updated_at": "2026-09-25T00:00:00Z",
        "pushed_at": "2026-09-25T01:00:00Z",
        "html_url": "https://github.com/octo/hello",
    }
    return repo | overrides


def client_for(handler) -> httpx.Client:
    """A real client, pointed at an in-memory fake GitHub."""
    return gr.make_client("test-token", transport=httpx.MockTransport(handler))


def ok(repo: dict, etag='"abc"', remaining="4999") -> httpx.Response:
    return httpx.Response(
        200, json=repo, headers={"etag": etag, "x-ratelimit-remaining": remaining}
    )


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(gr.time, "sleep", lambda s: None)


def test_slim_repo_keeps_attributes_and_metrics_and_drops_urls():
    slim = gr.slim_repo(repo_json(homepage=""))
    assert slim["full_name"] == "octo/hello"
    assert slim["owner_login"] == "octo" and slim["owner_type"] == "Organization"
    assert slim["license_spdx_id"] == "MIT"
    assert slim["topics"] == ["cli", "rust"]
    assert slim["stargazers_count"] == 42
    assert slim["homepage"] is None  # empty string normalised
    assert "html_url" not in slim and "avatar_url" not in str(slim)


def test_fetches_repo_and_records_etag_and_auth_headers():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["auth"] = request.headers.get("authorization")
        return ok(repo_json())

    records, counts = gr.run([QueueItem(1, "octo/hello")], client_for(handler))

    assert seen == {"path": "/repos/octo/hello", "auth": "Bearer test-token"}
    assert counts == {"ok": 1}
    (record,) = records
    assert record["http_status"] == 200 and record["etag"] == '"abc"'
    assert record["repo"]["language"] == "Rust"


def test_conditional_request_sends_etag_and_304_costs_no_quota():
    sent = []

    def handler(request):
        sent.append(request.headers.get("if-none-match"))
        return httpx.Response(304, headers={"x-ratelimit-remaining": "4999"})

    items = [QueueItem(i, f"octo/r{i}", etag=f'"e{i}"') for i in range(5)]
    records, counts = gr.run(items, client_for(handler), max_requests=2)

    assert sent == ['"e0"', '"e1"', '"e2"', '"e3"', '"e4"']
    assert counts == {"not_modified": 5}  # all processed: 304s don't count toward max_requests
    assert all(r["repo"] is None and r["etag"] == f'"e{r["repo_id"]}"' for r in records)


def test_renamed_repo_follows_redirect_to_new_name():
    def handler(request):
        if request.url.path == "/repos/old-owner/old-name":
            return httpx.Response(301, headers={"location": f"{gr.API_URL}/repositories/7"})
        assert request.url.path == "/repositories/7"
        return ok(repo_json(7, "new-owner/new-name"))

    records, _ = gr.run([QueueItem(7, "old-owner/old-name")], client_for(handler))

    assert records[0]["requested_name"] == "old-owner/old-name"
    assert records[0]["repo"]["full_name"] == "new-owner/new-name"


@pytest.mark.parametrize(("status", "expected"), [(404, "not_found"), (451, "blocked")])
def test_missing_and_blocked_repos_are_recorded(status, expected):
    records, counts = gr.run(
        [QueueItem(1, "octo/gone")],
        client_for(lambda r: httpx.Response(status, json={"message": "nope"})),
    )
    assert counts == {expected: 1}
    assert records[0]["http_status"] == status and records[0]["repo"] is None


def test_blocked_403_is_not_mistaken_for_rate_limit():
    def handler(request):
        return httpx.Response(
            403,
            json={"message": "Repository access blocked"},
            headers={"x-ratelimit-remaining": "4000"},
        )

    _, counts = gr.run([QueueItem(1, "octo/blocked")], client_for(handler))
    assert counts == {"blocked": 1}


def test_stops_when_rate_limited_and_leaves_the_rest():
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if len(calls) == 2:
            return httpx.Response(403, headers={"x-ratelimit-remaining": "0"})
        return ok(repo_json())

    items = [QueueItem(i, f"octo/r{i}") for i in range(4)]
    records, counts = gr.run(items, client_for(handler))

    assert len(calls) == 2 and len(records) == 1
    assert counts == {"ok": 1, "stopped_rate_limited": 1}


def test_stops_early_when_quota_runs_low():
    remaining = iter(["30", "26", "25", "24"])
    records, counts = gr.run(
        [QueueItem(i, f"octo/r{i}") for i in range(4)],
        client_for(lambda r: ok(repo_json(), remaining=next(remaining))),
        reserve=25,
    )
    assert len(records) == 3 and counts["stopped_low_quota"] == 1


def test_max_requests_caps_quota_spend():
    records, counts = gr.run(
        [QueueItem(i, f"octo/r{i}") for i in range(5)],
        client_for(lambda r: ok(repo_json())),
        max_requests=3,
    )
    assert len(records) == 3 and counts == {"ok": 3, "deferred_cap": 2}


def test_server_errors_are_retried():
    responses = iter([httpx.Response(502), ok(repo_json())])
    records, counts = gr.run([QueueItem(1, "octo/hello")], client_for(lambda r: next(responses)))
    assert counts == {"ok": 1} and len(records) == 1


def test_queue_file_and_local_landing_end_to_end(tmp_path: Path, monkeypatch):
    queue = tmp_path / "queue.jsonl"
    queue.write_bytes(
        b'{"repo_id": 1, "repo_name": "octo/hello"}\n'
        b'{"repo_id": 2, "repo_name": "octo/gone", "etag": null}\n'
    )

    def handler(request):
        if request.url.path == "/repos/octo/gone":
            return httpx.Response(404, json={"message": "Not Found"})
        return ok(repo_json())

    real_make_client = gr.make_client
    monkeypatch.setattr(
        gr, "make_client", lambda token: real_make_client(token, httpx.MockTransport(handler))
    )
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")

    exit_code = gr.main(["--queue-file", str(queue), "--output-dir", str(tmp_path / "out")])

    assert exit_code == 0
    (landed,) = (tmp_path / "out" / "repos").rglob("repos-*.json.gz")
    with gzip.open(landed, "rb") as f:
        rows = [orjson.loads(line) for line in f]
    assert [(r["repo_id"], r["http_status"]) for r in rows] == [(1, 200), (2, 404)]


def test_enrichment_runs_once_per_day_unless_forced(tmp_path: Path, monkeypatch):
    queue = tmp_path / "queue.jsonl"
    queue.write_bytes(b'{"repo_id": 1, "repo_name": "octo/hello"}\n')
    today_dir = tmp_path / "out" / "repos" / f"{datetime.now(UTC):%Y-%m-%d}"
    today_dir.mkdir(parents=True)
    (today_dir / "repos-earlier-run.json.gz").write_bytes(b"landed by the first run")

    clients = []
    real_make_client = gr.make_client

    def counting_make_client(token):
        clients.append(token)
        return real_make_client(token, httpx.MockTransport(lambda r: ok(repo_json())))

    monkeypatch.setattr(gr, "make_client", counting_make_client)
    args = ["--queue-file", str(queue), "--output-dir", str(tmp_path / "out")]

    # The backup run finds today's file and stops before touching the API.
    assert gr.main(args) == 0
    assert clients == []
    assert len(list(today_dir.iterdir())) == 1

    # --force runs anyway and lands a second file.
    assert gr.main([*args, "--force"]) == 0
    assert len(clients) == 1
    assert len(list(today_dir.iterdir())) == 2
