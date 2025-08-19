import base64
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple


def _get_workspace_host_from_context() -> Optional[str]:
    """Try to derive the workspace host URL from the notebook context."""
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
        # Example: https://adb-123456789012.3.azuredatabricks.net
        return ctx
    except Exception:
        return None


def get_workspace_client(
    dbutils,
    host: Optional[str] = None,
    secret_scope: str = "kv-scope",
    secret_key: str = "databricks-token",
):
    """
    Initialize and return a Databricks WorkspaceClient with
    authentication.

    Resolution order for credentials (best-practice):
    - Use DATABRICKS_HOST and DATABRICKS_TOKEN env vars if present
    - Else retrieve PAT from Databricks Secrets (dbutils.secrets)
      and derive host from context
    - Allow explicit `host` override
    """
    try:
        from databricks.sdk import WorkspaceClient
    except Exception as exc:
        raise ImportError(
            "databricks-sdk is required. Install with `pip install "
            "databricks-sdk`."
        ) from exc

    resolved_host = _get_workspace_host_from_context()

    try:
        resolved_token = dbutils.secrets.get(
            scope=secret_scope, key=secret_key
        )
    except Exception as exc:
        raise RuntimeError(
            "Failed to retrieve token from Databricks Secrets. "
            "Ensure the secret scope and key exist or set "
            f"DATABRICKS_TOKEN env var.{exc}"
        ) from exc

        if not resolved_host or not resolved_token:
            raise RuntimeError(
                "Missing Databricks credentials. Provide host/token via "
                "environment variables, explicit parameters, or "
                "Databricks secrets."
            )

    # Prefer explicit host+token to avoid surprises; WorkspaceClient()
    # also works if env is set
    return WorkspaceClient(host=resolved_host, token=resolved_token)


def _decode_export_response(export_response: Any) -> bytes:
    """
    Decode the SDK `workspace.export` response into raw bytes.

    Supports both base64-encoded content payloads and direct bytes.
    """
    # Newer SDKs return a typed object with a `content` base64 field
    # when direct_download=False
    content_attr = getattr(export_response, "content", None)
    if content_attr:
        try:
            return base64.b64decode(content_attr)
        except Exception:
            # If already bytes/str, fall through
            pass

    # Some versions may return bytes/bytearray directly
    if isinstance(export_response, (bytes, bytearray)):
        return bytes(export_response)

    # As a last resort, treat as UTF-8 string
    if isinstance(export_response, str):
        return export_response.encode("utf-8")

    raise ValueError(
        "Unsupported export response type; unable to decode content"
    )


def export_notebook(
    client: Any,
    notebook_path: str,
    prefer_jupyter: bool = True,
) -> Dict[str, Any]:
    """
    Export a workspace notebook.

    Returns a dict with keys:
    - format: 'JUPYTER' or 'SOURCE'
    - content_bytes: raw bytes of the exported file
    """
    try:
        from databricks.sdk.service.workspace import ExportFormat
    except Exception as exc:
        raise ImportError(
            "databricks-sdk is required. Install with `pip install "
            "databricks-sdk`."
        ) from exc

    # Try JUPYTER (cell-structured) first for robust parsing
    if prefer_jupyter:
        try:
            res = client.workspace.export(
                path=notebook_path,
                format=ExportFormat.JUPYTER,
            )
            return {
                "format": "JUPYTER",
                "content_bytes": _decode_export_response(res),
            }
        except Exception:
            # Fall back to SOURCE if JUPYTER not available for this notebook
            pass

    res = client.workspace.export(
        path=notebook_path,
        format=ExportFormat.SOURCE,
    )
    return {
        "format": "SOURCE",
        "content_bytes": _decode_export_response(res),
    }


def _strip_magic_prefix(text: str, magic: str) -> str:
    pattern = rf"^\s*{re.escape(magic)}\s*\n?"
    return re.sub(pattern, "", text, count=1, flags=re.IGNORECASE)


def _detect_cell_type_and_normalize(content: str) -> Tuple[str, str]:
    """
    Infer cell type from magic commands and normalize content by
    removing magics.
    """
    stripped = content.lstrip()
    if stripped.startswith("%%sql"):
        return "sql", _strip_magic_prefix(stripped, "%%sql").lstrip()
    if stripped.startswith("%sql"):
        return "sql", _strip_magic_prefix(stripped, "%sql").lstrip()
    if stripped.startswith("%%python"):
        return "python", _strip_magic_prefix(stripped, "%%python").lstrip()
    if stripped.startswith("%python"):
        return "python", _strip_magic_prefix(stripped, "%python").lstrip()
    return "python", content


def parse_jupyter_cells(
    content_bytes: bytes,
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Parse an IPYNB (JUPYTER) notebook export into
    (cell_type, content, metadata) tuples.
    """
    data = json.loads(content_bytes.decode("utf-8"))
    cells: List[Tuple[str, str, Dict[str, Any]]] = []
    for cell in data.get("cells", []):
        cell_type = cell.get("cell_type", "code")
        source_lines = cell.get("source", [])
        content = "".join(source_lines)
        if cell_type == "code":
            detected_type, normalized = _detect_cell_type_and_normalize(
                content
            )
            cells.append((detected_type, normalized, {"jupyter": True}))
        elif cell_type == "markdown":
            # Skip markdown
            continue
        else:
            # Treat unknown types as python code by default
            cells.append(
                (
                    "python",
                    content,
                    {"jupyter": True, "original_type": cell_type},
                )
            )
    return cells


def parse_source_cells(
    content_bytes: bytes,
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Parse a Databricks SOURCE export by splitting on
    `# COMMAND ----------` cell markers.
    """
    text = content_bytes.decode("utf-8", errors="replace")
    # Split on exact Databricks cell delimiter
    parts = re.split(r"(?m)^# COMMAND ----------\s*$", text)
    cells: List[Tuple[str, str, Dict[str, Any]]] = []
    for raw in parts:
        # Ignore header lines commonly present in SOURCE export
        cleaned = re.sub(r"(?m)^# Databricks notebook source\s*\n", "", raw)
        cleaned = cleaned.strip()
        if not cleaned:
            continue
        detected_type, normalized = _detect_cell_type_and_normalize(cleaned)
        cells.append((detected_type, normalized, {"jupyter": False}))
    return cells


def extract_cells(
    exported: Dict[str, Any]
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Convert an exported notebook payload into a list of
    (cell_type, content, metadata).

    Cell types: 'sql' or 'python'. Magic commands like `%sql` and `%%sql`
    are detected and removed.
    """
    fmt = exported.get("format")
    content_bytes: bytes = exported.get("content_bytes", b"")

    if fmt == "JUPYTER":
        return parse_jupyter_cells(content_bytes)
    if fmt == "SOURCE":
        return parse_source_cells(content_bytes)
    # Unknown format: try SOURCE parser as a fallback
    return parse_source_cells(content_bytes)
