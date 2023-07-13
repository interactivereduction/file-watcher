"""
E2E Test
"""
import shutil
import time
from pathlib import Path

import pytest
from memphis import Memphis


@pytest.fixture(autouse=True, scope="module")
def setup_and_teardown():
    """
    Set up the test archive dir and remove it post test
    :return: None
    """
    if not Path("test_archive").exists():
        Path("test_archive").mkdir()

    yield

    shutil.rmtree("test_archive", ignore_errors=True)


@pytest.mark.asyncio
async def assert_files_arrived() -> None:
    """
    Assert the expected files arrived at the memphis station
    :return:
    """
    memphis = Memphis()
    await memphis.connect(host="localhost", username="root", password="memphis")
    recieved = [
        message.get_data().decode() for message in await memphis.fetch_messages("watched-files", "e2e_consumer")
    ]
    try:
        assert len(recieved) == 2
        assert "/archive/some_file.nxs" in recieved
        assert "/archive/some_other_dir/foo.nxs" in recieved
    finally:
        await memphis.close()


def create_test_files_and_dirs():
    """
    Create files and directories used to test
    :return: None
    """
    Path("test_archive/some_dir").mkdir()
    Path("test_archive/some_file.nxs").touch()
    Path("test_archive/some_other_dir").mkdir()
    Path("test_archive/some_other_dir/foo.nxs").touch()


@pytest.mark.asyncio
async def test_created_files_result_in_sent_messages():
    """
    Test creation of files and dirs and verify they arrive on the station
    :return: None
    """
    create_test_files_and_dirs()

    time.sleep(15)

    await assert_files_arrived()
