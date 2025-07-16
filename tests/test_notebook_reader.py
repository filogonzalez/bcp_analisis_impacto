import sys
import os
import pytest
import pandas as pd

notebook_reader_module_path = (
    '/Workspace/Users/filo.gzz@databricks.com/.bundle/bcp_analisis_impacto/'
    'dev/files/src/'
)
sys.path.append(os.path.abspath(notebook_reader_module_path))
from notebook_reader import NotebookPathReader, ExcelFileError

@pytest.fixture
def sample_xlsx(tmp_path):
    df = pd.DataFrame({
        'Path': [' /Workspace/Notebook1 ', '', ' /Workspace/Notebook2', None],
        'Other': [1, 2, 3, 4]
    })
    file = tmp_path / 'sample.xlsx'
    df.to_excel(file, index=False, engine='openpyxl')
    return str(file)


@pytest.fixture
def sample_csv(tmp_path):
    df = pd.DataFrame({
        'notebook_path': ['NotebookA', ' ', None, 'NotebookB']
    })
    file = tmp_path / 'sample.csv'
    df.to_csv(file, index=False)
    return str(file)


def test_read_from_excel_infers_column(sample_xlsx):
    reader = NotebookPathReader()
    paths = reader.read_from_excel(sample_xlsx)
    assert paths == ['/Workspace/Notebook1', '/Workspace/Notebook2']


def test_read_from_excel_explicit_column(sample_xlsx):
    reader = NotebookPathReader()
    paths = reader.read_from_excel(sample_xlsx, path_column='Path')
    assert paths == ['/Workspace/Notebook1', '/Workspace/Notebook2']


def test_read_from_csv_infers_column(sample_csv):
    reader = NotebookPathReader()
    paths = reader.read_from_excel(sample_csv)
    assert paths == ['NotebookA', 'NotebookB']


def test_read_from_csv_explicit_column(sample_csv):
    reader = NotebookPathReader()
    paths = reader.read_from_excel(sample_csv, path_column='notebook_path')
    assert paths == ['NotebookA', 'NotebookB']


def test_missing_path_column_raises(tmp_path):
    df = pd.DataFrame({'foo': [1, 2]})
    file = tmp_path / 'no_path.xlsx'
    df.to_excel(file, index=False, engine='openpyxl')
    reader = NotebookPathReader()
    with pytest.raises(ExcelFileError):
        reader.read_from_excel(str(file))


def test_validate_paths_against_workspace(monkeypatch):
    reader = NotebookPathReader()
    paths = ["/Workspace/Notebook1", "/Workspace/DoesNotExist"]
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        pytest.skip("databricks-sdk not installed")
        return

    class MockWorkspace:
        def get_status(self, path):
            if path == "/Workspace/Notebook1":
                return True
            raise WorkspaceClient.NotFound("not found")

    class MockClient:
        def __init__(self):
            self.workspace = MockWorkspace()
    result = reader.validate_paths_against_workspace(
        paths, workspace_client=MockClient()
    )
    assert isinstance(result, dict)
    for path in paths:
        assert path in result
        entry = result[path]
        assert isinstance(entry, dict)
        assert "exists" in entry
        assert isinstance(entry["exists"], bool)
        assert "error" in entry
        # For the known valid/invalid paths, check expected values
        if path == "/Workspace/Notebook1":
            assert entry["exists"] is True
            assert entry["error"] is None
        elif path == "/Workspace/DoesNotExist":
            assert entry["exists"] is False
            assert entry["error"] is not None