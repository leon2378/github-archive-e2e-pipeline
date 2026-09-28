"""Lakeflow Job task: enrich the queued repos from the GitHub REST API.

Same wrapper as ingest_gharchive.py, plus the GitHub token, read from the `gharchive` secret
scope. Without a token the task still runs, but GitHub allows only 60 requests an hour, so the
job stops early and leaves the rest of the queue for the next run.
"""

import os
import sys

sys.path.insert(0, sys.argv[1])

from databricks.sdk.runtime import dbutils  # noqa: E402

from ingestion.github_repos import main  # noqa: E402

try:
    os.environ["GITHUB_TOKEN"] = dbutils.secrets.get(scope="gharchive", key="github_token")
except Exception as exc:
    print(f"WARNING: no GitHub token in secret gharchive/github_token ({exc})")

exit_code = main(sys.argv[2:])
if exit_code:
    sys.exit(exit_code)
