"""
Example usage:

from notebook_reader import DatabricksSecretProvider
from notebook_api import DatabricksNotebookClient

# Initialize the secret provider (adjust scope/key names as needed)
secret_provider = DatabricksSecretProvider(
    scope="DEFAULT",  # Replace with your secret scope name
    token_key="DATABRICKS_TOKEN",  # Replace with your token key
    url_key="DATABRICKS_HOST"  # Replace with your host key
)

# Initialize the notebook client
client = DatabricksNotebookClient(auth_provider=secret_provider)

# Retrieve notebook content by path
notebook_path = "/Workspace/Path/To/Notebook"  # Replace with your notebook path
try:
    content = client.get_notebook_content(path=notebook_path, format="SOURCE")
    print("Notebook content (first 200 chars):\n", str(content)[:200])
except Exception as e:
    print(f"Failed to retrieve notebook: {e}")

"""
import logging
import time
from typing import Optional, Any

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound, DatabricksError
except ImportError:
    WorkspaceClient = None  # type: ignore
    NotFound = Exception
    DatabricksError = Exception


class NotebookRetrievalError(Exception):
    """Raised when notebook retrieval fails after retries."""


class DatabricksNotebookClient:
    def __init__(
        self,
        auth_provider: Any,
        log_level: int = logging.INFO,
        max_retries: int = 3,
        backoff_base: float = 0.5,
        timeout: Optional[float] = None,
    ):
        """
        Initialize the DatabricksNotebookClient.
        :param auth_provider: An object with get_token() and get_host() methods.
        :param log_level: Logging level.
        :param max_retries: Max number of retries for API calls.
        :param backoff_base: Base for exponential backoff.
        :param timeout: Request timeout in seconds (if supported).
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s %(levelname)s %(name)s: %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(log_level)
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.timeout = timeout
        self.auth_provider = auth_provider
        self._workspace_client = None
        self._init_workspace_client()

    def _init_workspace_client(self):
        if WorkspaceClient is None:
            raise ImportError("databricks-sdk is required for notebook API access.")
        token = self.auth_provider.get_token()
        host = self.auth_provider.get_host()
        self._workspace_client = WorkspaceClient(
            host=host,
            token=token,
            product="notebook_api_client"
        )
        self.logger.info(
            "Initialized WorkspaceClient for "
            "Databricks API access."
        )

    def get_notebook_content(
        self,
        path: Optional[str] = None,
        object_id: Optional[str] = None,
        format: str = "SOURCE"
    ) -> Any:
        """
        Retrieve notebook content by path or object ID.
        :param path: Workspace path to the notebook.
        :param object_id: Workspace object ID (alternative to path).
        :param format: Export format: SOURCE, HTML, JUPYTER, DBC.
        :return: Notebook content (raw or parsed, depending on format).
        """
        if not path and not object_id:
            raise ValueError(
                "Either path or object_id must be provided."
            )
        if format not in {"SOURCE", "HTML", "JUPYTER", "DBC"}:
            raise ValueError(
                f"Unsupported export format: {format}"
            )
        attempt = 0
        last_exception = None
        while attempt < self.max_retries:
            try:
                if path:
                    self.logger.info(
                        f"Retrieving notebook at path: {path} "
                        f"(format={format})"
                    )
                    content = self._workspace_client.workspace.export(
                        path=path, format=format
                    )
                else:
                    self.logger.info(
                        f"Retrieving notebook by object_id: {object_id} "
                        f"(format={format})"
                    )
                    content = self._workspace_client.workspace.export(
                        object_id=object_id, format=format
                    )
                self.logger.info(
                    "Notebook content retrieved successfully."
                )
                return self._parse_notebook_content(content, format)
            except NotFound as e:
                self.logger.error(f"Notebook not found: {e}")
                raise
            except DatabricksError as e:
                self.logger.warning(
                    f"Databricks API error: {e} (attempt {attempt+1})"
                )
                last_exception = e
            except Exception as e:
                self.logger.warning(
                    f"Unexpected error: {e} (attempt {attempt+1})"
                )
                last_exception = e
            sleep_time = self.backoff_base * (2 ** attempt)
            self.logger.info(
                f"Retrying after {sleep_time:.2f}s..."
            )
            time.sleep(sleep_time)
            attempt += 1
        self.logger.error(
            "Failed to retrieve notebook after retries."
        )
        raise NotebookRetrievalError(
            f"Failed to retrieve notebook: {last_exception}"
        )

    def _parse_notebook_content(self, content: Any, format: str) -> Any:
        """
        Parse notebook content based on export format.
        For now, returns raw content. Extend for format-specific parsing.
        """
        # TODO: Implement format-specific parsing (SOURCE, HTML, JUPYTER, DBC)
        return content 