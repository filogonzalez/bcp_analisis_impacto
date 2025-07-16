"""
notebook_reader.py

Provides NotebookPathReader for extracting notebook paths from Excel/CSV files.

Example usage:
    from notebook_reader import NotebookPathReader
    reader = NotebookPathReader()
    paths = reader.read_from_excel(
        'notebooks.xlsx', sheet_name='Sheet1', path_column='Path')
"""

# notebook_reader.py
import pandas as pd
from typing import List, Optional, Union, Dict, Set
import os
import logging
import time
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound
except ImportError:
    WorkspaceClient = None  # type: ignore
    NotFound = Exception


class ExcelFileError(Exception):
    """Raised when there is an error reading the Excel/CSV file."""


class PathValidationError(Exception):
    """Raised when there is an error validating notebook paths."""


class NotebookPathReader:
    def __init__(self, log_level: int = logging.INFO):
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s %(levelname)s %(name)s: %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(log_level)

    def read_from_excel(
        self,
        file_path: str,
        sheet_name: Optional[Union[str, int]] = 0,
        path_column: Optional[str] = None,
        skiprows: Optional[int] = None,
        **kwargs,
    ) -> List[str]:
        """
        Read notebook paths from Excel/CSV file. Raises ExcelFileError on failure.
        """
        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext in [".xlsx", ".xls"]:
                df = pd.read_excel(
                    file_path,
                    sheet_name=sheet_name,
                    skiprows=skiprows,
                    engine="openpyxl" if ext == ".xlsx" else None,
                    **kwargs,
                )
            elif ext == ".csv":
                df = pd.read_csv(file_path, skiprows=skiprows, **kwargs)
            else:
                raise ExcelFileError(
                    f"Unsupported file extension: {ext}"
                )
        except Exception as e:
            raise ExcelFileError(
                f"Failed to read file {file_path}: {e}"
            ) from e

        if path_column is None:
            candidates = [c for c in df.columns if "path" in c.lower()]
            if not candidates:
                raise ExcelFileError(
                    "Could not infer path column. "
                    "Please specify 'path_column'."
                )
            path_column = candidates[0]

        try:
            series = (
                df[path_column]
                .dropna()
                .astype(str)
                .str.strip()
            )
            series = series[
                (~series.str.lower().isin(["", "nan", "none"]))
            ]
        except Exception as e:
            raise ExcelFileError(
                f"Failed to extract notebook paths: {e}"
            ) from e

        return series.tolist()

    def validate_paths_against_workspace(
        self,
        paths: List[str],
        cache: Optional[Set[str]] = None,
        batch_size: int = 20,
        workspace_client: Optional[object] = None,
        max_retries: int = 3,
        backoff_base: float = 0.5,
    ) -> Dict[str, Dict]:
        """
        Validate notebook paths against the Databricks workspace using the SDK.
        Returns a dict mapping path -> {exists: bool, error: str or None}.
        Logs summary and telemetry. Handles partial failures gracefully.
        """
        if WorkspaceClient is None:
            raise ImportError(
                "databricks-sdk is required for workspace validation."
            )
        w = workspace_client or WorkspaceClient()
        cache = cache or set()
        result = {}
        api_calls = 0
        start_time = time.time()
        for path in paths:
            norm_path = self._normalize_path(path)
            if norm_path in cache:
                result[path] = {"exists": True, "error": None}
                continue
            attempt = 0
            while attempt < max_retries:
                try:
                    api_calls += 1
                    w.workspace.get_status(norm_path)
                    result[path] = {"exists": True, "error": None}
                    cache.add(norm_path)
                    break
                except NotFound:
                    result[path] = {"exists": False, "error": None}
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        sleep_time = backoff_base * (2 ** attempt)
                        self.logger.warning(
                            f"Retrying path '{norm_path}' due to error: {e} "
                            f"(attempt {attempt+1})"
                        )
                        time.sleep(sleep_time)
                        attempt += 1
                    else:
                        self.logger.error(
                            f"Failed to validate path '{norm_path}': {e}"
                        )
                        result[path] = {"exists": False, "error": str(e)}
                        break
        elapsed = time.time() - start_time
        valid = [p for p, v in result.items() if v["exists"]]
        invalid = [
            p for p, v in result.items()
            if not v["exists"] and v["error"] is None
        ]
        errors = [p for p, v in result.items() if v["error"]]
        self.logger.info(
            f"Validation complete: {len(valid)} valid, {len(invalid)} invalid, "
            f"{len(errors)} errors. API calls: {api_calls}. "
            f"Time: {elapsed:.2f}s"
        )
        return result

    def _normalize_path(self, path: str) -> str:
        """
        Normalize workspace paths (strip, ensure leading slash, remove trailing
        spaces).
        """
        p = path.strip()
        if not p.startswith("/"):
            p = "/" + p
        return p


class DatabricksSecretProvider:
    """
    Utility class to retrieve Databricks secrets (e.g., tokens) from secret scopes.
    Falls back to environment variables if dbutils is not available.
    """
    def __init__(self, scope: str = "DEFAULT", token_key: str = "DATABRICKS_TOKEN", url_key: str = "DATABRICKS_HOST", logger: Optional[logging.Logger] = None):
        self.scope = scope
        self.token_key = token_key
        self.url_key = url_key
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def get_token(self) -> str:
        try:
            import dbutils  # type: ignore
            token = dbutils.secrets.get(
                scope=self.scope, key=self.token_key
            )
            self.logger.info(
                "Retrieved Databricks token from secret scope."
            )
            return token
        except Exception as e:
            self.logger.warning(
                f"dbutils.secrets.get failed: {e}. "
                f"Falling back to environment variable."
            )
            token = os.environ.get(self.token_key)
            if not token:
                raise RuntimeError(
                    f"Databricks token not found in secret scope or "
                    f"environment variable '{self.token_key}'"
                )
            return token

    def get_host(self) -> str:
        try:
            import dbutils  # type: ignore
            host = dbutils.secrets.get(
                scope=self.scope, key=self.url_key
            )
            self.logger.info(
                "Retrieved Databricks host from secret scope."
            )
            return host
        except Exception as e:
            self.logger.warning(
                f"dbutils.secrets.get failed: {e}. "
                f"Falling back to environment variable."
            )
            host = os.environ.get(self.url_key)
            if not host:
                raise RuntimeError(
                    f"Databricks host not found in secret scope or "
                    f"environment variable '{self.url_key}'"
                )
            return host

# Placeholder for DatabricksNotebookClient integration
# from notebook_api import DatabricksNotebookClient
# client = DatabricksNotebookClient(auth_provider=DatabricksSecretProvider())
