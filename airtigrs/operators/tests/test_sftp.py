import pytest
import datetime
import pytz
import os
import pysftp

from airtigrs.plugins.operators.sftp import SFTPFetchOperator
from airtigrs.utils.dates import as_datetime
from airflow.models import Connection

from airflow.exceptions import AirflowSkipException

DATA_INTERVAL_START = datetime.datetime(2021, 9, 13, tzinfo=pytz.UTC)
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)
MOCK_SFTP_CONN_ID = "mock_sftp"
MOCK_SFTP_TASK_ID = "mock_sftp_id"
MOCK_SFTP_DAG_ID = "mock_sftp_task"


def get_task(local_path=None, remote_path=None):
    return SFTPFetchOperator(sftp_conn_id=MOCK_SFTP_CONN_ID,
                             task_id=MOCK_SFTP_TASK_ID,
                             local_path=local_path,
                             remote_path=remote_path)


@pytest.fixture(scope="function")
def local_file(request, tmp_path):

    mtime = request.param
    f = tmp_path / "local" / "test_file"
    f.parent.mkdir()
    f.touch()

    # Modified mtime
    os.utime(str(f), (100, mtime))
    return f


@pytest.fixture(scope="function")
def remote_file(request, tmp_path):

    mtime = request.param
    f = tmp_path / "remote" / "test_file"
    f.parent.mkdir()
    f.touch()

    # Modified mtime
    os.utime(str(f), (100, mtime))
    return f


def test_exception_when_remote_path_not_provided(mocker):
    conn = Connection(conn_type="sftp", login="test", host="mock")
    conn_uri = conn.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_MOCK_SFTP=conn_uri)
    task = get_task()

    with pytest.raises(KeyError):
        task.execute(context={})


@pytest.mark.parametrize("remote_file", [100], indirect=["remote_file"])
def test_sftp_get_called_when_missing_files_locally(remote_file, tmp_path,
                                                    mocker):
    '''
    Check remote SFTP
    '''
    local_dir = tmp_path / "local"
    local_dir.mkdir()

    mocker.patch('pysftp.Connection')
    mocker.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.list_directory',
                 wraps=os.listdir)

    conn = Connection(conn_type="sftp", login="test", host="test")
    conn_uri = conn.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_MOCK_SFTP=conn_uri)
    task = get_task(local_path=local_dir, remote_path=str(remote_file.parent))
    task.execute(context={})

    expected = {
        "remotepath": 'test_file',
        "localpath": "tests/test_sftp_operator/missing_file/local/test_file",
        "preserve_mtime": True
    }
    pysftp.Connection().__enter__().get.called_once_with(**expected)


@pytest.mark.parametrize("local_file,remote_file", [(100, 100)],
                         indirect=["local_file", "remote_file"])
def test_sftp_get_not_called_if_no_updates(local_file, remote_file, mocker):
    mocker.patch('pysftp.Connection')
    mocker.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.list_directory',
                 wraps=os.listdir)
    mocker.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_mod_time',
                 wraps=lambda x: as_datetime(os.path.getmtime(x)))

    conn = Connection(conn_type="sftp", login="test", host="test")
    conn_uri = conn.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_MOCK_SFTP=conn_uri)

    task = get_task(local_path=str(local_file.parent),
                    remote_path=str(remote_file.parent))

    with pytest.raises(AirflowSkipException):
        task.execute(context={})


@pytest.mark.parametrize("local_file,remote_file", [(100, 200)],
                         indirect=["local_file", "remote_file"])
def test_sftp_get_called_if_update_needed(local_file, remote_file, mocker):

    mocker.patch('pysftp.Connection')
    mocker.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.list_directory',
                 wraps=os.listdir)
    mocker.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_mod_time',
                 wraps=lambda x: as_datetime(os.path.getmtime(x)))

    conn = Connection(conn_type="sftp", login="test", host="test")
    conn_uri = conn.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_MOCK_SFTP=conn_uri)

    task = get_task(local_path=str(local_file.parent),
                    remote_path=str(remote_file.parent))
    task.execute(context={})

    expected = {
        "remotepath": 'test_file',
        "localpath": "tests/test_sftp_operator/updating_file/local/test_file",
        "preserve_mtime": True
    }

    pysftp.Connection().__enter__().get.called_once_with(**expected)
