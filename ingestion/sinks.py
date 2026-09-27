"""Where ingestion jobs land their files: a Unity Catalog volume, or a local folder for dry runs."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Protocol


class Sink(Protocol):
    def exists(self, rel_path: str) -> bool: ...
    def has_files(self, rel_dir: str) -> bool: ...
    def put(self, local: Path, rel_path: str) -> None: ...


class VolumeSink:
    """Writes into a Unity Catalog volume through the Databricks Files API."""

    def __init__(self, volume_path: str) -> None:
        # Imported here so local-only runs and tests don't need Databricks credentials.
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.errors import NotFound

        self._not_found = NotFound
        self.root = volume_path.rstrip("/")
        # Auth comes from DATABRICKS_HOST/DATABRICKS_TOKEN or your ~/.databrickscfg profile.
        self.client = WorkspaceClient()
        try:
            self.client.files.get_directory_metadata(self.root)
        except NotFound:
            raise SystemExit(
                f"Volume {self.root} not found. Run `databricks bundle deploy` first."
            ) from None

    def exists(self, rel_path: str) -> bool:
        try:
            self.client.files.get_metadata(f"{self.root}/{rel_path}")
        except self._not_found:
            return False
        return True

    def has_files(self, rel_dir: str) -> bool:
        try:
            entries = self.client.files.list_directory_contents(f"{self.root}/{rel_dir}")
            return any(not entry.is_directory for entry in entries)
        except self._not_found:
            return False

    def put(self, local: Path, rel_path: str) -> None:
        with local.open("rb") as f:
            self.client.files.upload(f"{self.root}/{rel_path}", f, overwrite=True)


class LocalSink:
    """Writes into a local directory, for trying the ingestion without Databricks."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def exists(self, rel_path: str) -> bool:
        return (self.root / rel_path).exists()

    def has_files(self, rel_dir: str) -> bool:
        folder = self.root / rel_dir
        return folder.is_dir() and any(p.is_file() for p in folder.iterdir())

    def put(self, local: Path, rel_path: str) -> None:
        target = self.root / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local, target)
