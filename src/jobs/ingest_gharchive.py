"""Lakeflow Job task: land new GH Archive hours in the Unity Catalog volume.

Runs ingestion/gharchive.py on Databricks serverless compute. The bundle syncs the repository to
the workspace; the first argument is that files root, so the `ingestion` package imports exactly
as it does locally and in tests. The remaining arguments go straight to its command line.
"""

import sys

sys.path.insert(0, sys.argv[1])

from ingestion.gharchive import main  # noqa: E402

exit_code = main(sys.argv[2:])
if exit_code:
    sys.exit(exit_code)
