import pytest
import datetime
import pytz
import os
import pysftp

from airtigrs.plugins.operators.sftp import SFTPFetchOperator
from airtigrs.utils import as_datetime
from airflow.models import Connection

from airflow.exceptions import AirflowSkipException

FIXTURE_DIR = "tests/test_sftp_operator"
DATA_INTERVAL_START = datetime.datetime(2021, 9, 13, tzinfo=pytz.UTC)
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)
MOCK_SFTP_CONN_ID = "mock_sftp"
MOCK_SFTP_TASK_ID = "mock_sftp_id"
MOCK_SFTP_DAG_ID = "mock_sftp_task"


@pytest.fixture(scope="function")
def task(request):
    local_path = None
    remote_path = None
    if request.param is not None:
        local_path = os.path.join(FIXTURE_DIR, request.param, 'local')
        remote_path = os.path.join(FIXTURE_DIR, request.param, 'remote')
    return SFTPFetchOperator(sftp_conn_id=MOCK_SFTP_CONN_ID,
                             task_id=MOCK_SFTP_TASK_ID,
                             local_path=local_path,
                             remote_path=remote_path)


@pytest.mark.parametrize("task", [None], indirect=["task"])
def test_exception_when_remote_path_not_provided(task, mocker):
    conn = Connection(conn_type="sftp", login="test", host="mock")
    conn_uri = conn.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_MOCK_SFTP=conn_uri)

    with pytest.raises(KeyError):
        task.execute(context={})


@pytest.mark.parametrize("task", ["missing_file"], indirect=["task"])
def test_sftp_get_called_when_missing_files_locally(task, mocker):
    '''
    Check remote SFTP
    '''
    mocker.patch('pysftp.Connection')
    mocker.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.list_directory',
                 wraps=os.listdir)

    conn = Connection(conn_type="sftp", login="test", host="test")
    conn_uri = conn.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_MOCK_SFTP=conn_uri)
    task.execute(context={})

    expected = {
        "remotepath": 'test_file',
        "localpath": "tests/test_sftp_operator/missing_file/local/test_file",
        "preserve_mtime": True
    }
    pysftp.Connection().__enter__().get.called_once_with(**expected)


@pytest.mark.parametrize("task", ["no_updates"], indirect=["task"])
def test_sftp_get_not_called_if_no_updates(task, mocker):
    mocker.patch('pysftp.Connection')
    mocker.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.list_directory',
                 wraps=os.listdir)
    mocker.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_mod_time',
                 wraps=lambda x: as_datetime(os.path.getmtime(x)))

    conn = Connection(conn_type="sftp", login="test", host="test")
    conn_uri = conn.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_MOCK_SFTP=conn_uri)

    with pytest.raises(AirflowSkipException):
        task.execute(context={})


@pytest.mark.parametrize("task", ["updating_file"], indirect=["task"])
def test_sftp_get_called_if_update_needed(task, mocker):
    mocker.patch('pysftp.Connection')
    mocker.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.list_directory',
                 wraps=os.listdir)
    mocker.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_mod_time',
                 wraps=lambda x: as_datetime(os.path.getmtime(x)))

    conn = Connection(conn_type="sftp", login="test", host="test")
    conn_uri = conn.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_MOCK_SFTP=conn_uri)
    task.execute(context={})

    expected = {
        "remotepath": 'test_file',
        "localpath": "tests/test_sftp_operator/updating_file/local/test_file",
        "preserve_mtime": True
    }
    pysftp.Connection().__enter__().get.called_once_with(**expected)
