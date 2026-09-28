"""Lakeflow Job task: enrich the queued repos from the GitHub REST API.

Same wrapper as ingest_gharchive.py, plus the GitHub token, read from the `gharchive` secret
scope. The token is cleaned before it goes into the environment: a stray control character from
pasting it (such as a NUL) would otherwise make os.environ reject it. Without a usable token the
task still runs, but GitHub allows only 60 requests an hour, so it stops early and leaves the
rest of the queue for the next run.
"""

import os
import sys

sys.path.insert(0, sys.argv[1])

from databricks.sdk.runtime import dbutils  # noqa: E402

from ingestion.github_repos import clean_token, main  # noqa: E402

try:
    token = clean_token(dbutils.secrets.get(scope="gharchive", key="github_token"))
except Exception as exc:
    token = None
    print(f"WARNING: could not read secret gharchive/github_token ({exc})")

if token:
    os.environ["GITHUB_TOKEN"] = token
else:
    print("WARNING: no usable GitHub token in secret gharchive/github_token")

exit_code = main(sys.argv[2:])
if exit_code:
    sys.exit(exit_code)
