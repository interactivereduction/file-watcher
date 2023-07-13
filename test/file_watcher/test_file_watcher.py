"""
Main test cases
"""
import asyncio
import os
from queue import SimpleQueue
from unittest.mock import patch, Mock, AsyncMock

import pytest

from main.file_watcher import watch, start_watching, setup_producer, main, load_config


@patch("file_watcher.file_watcher.asyncio.sleep")
def test_watch_no_file(mock_sleep) -> None:
    """
    Test watch loop when no file in queue
    :param mock_sleep: mock asyncio sleep
    :return: None
    """
    queue = Mock()
    queue.empty.return_value = True
    producer = Mock()
    mock_sleep.side_effect = InterruptedError
    with pytest.raises(InterruptedError):
        asyncio.run(watch(queue, producer))
    queue.empty.assert_called_once()
    producer.produce.assert_not_called()
    mock_sleep.assert_called_once_with(0.4)


@patch("file_watcher.file_watcher.asyncio.sleep")
def test_watch_with_file(mock_sleep) -> None:
    """
    Test watch loop when file in queue
    :param mock_sleep: mock asyncio sleep
    :return: None
    """
    queue = Mock()
    file = "foo.nxs"
    queue.empty.return_value = False
    queue.get.return_value = file
    producer = AsyncMock()
    mock_sleep.side_effect = InterruptedError
    with pytest.raises(InterruptedError):
        asyncio.run(watch(queue, producer))
    queue.empty.assert_called_once()
    producer.produce.assert_called_once_with(file)
    mock_sleep.assert_called_once_with(0.4)


@patch("file_watcher.file_watcher.PollingObserver")
@patch("file_watcher.file_watcher.QueueBasedEventHandler")
def test_setup_watcher(mock_handler_class, mock_observer_class):
    """
    Test the watcher setup
    :param mock_handler_class: class mock for handler
    :param mock_observer_class: class mock for observer
    :return: None
    """
    queue = SimpleQueue()
    config = Mock()
    mock_handler = Mock()
    mock_handler_class.return_value = mock_handler
    mock_observer = Mock()
    mock_observer_class.return_value = mock_observer
    start_watching(queue, config)
    mock_handler_class.assert_called_once_with(queue)
    mock_observer_class.assert_called_once()
    mock_observer.schedule.assert_called_once_with(mock_handler, config.watch_dir)
    mock_observer.start.assert_called_once()


@patch("file_watcher.file_watcher.Memphis")
def test_setup_producer(mock_memphis_class):
    """
    Test producer_setup
    :param mock_memphis_class: Mock class for memphis
    :return: None
    """
    memphis = AsyncMock()
    mock_memphis_class.return_value = memphis
    producer = Mock()
    memphis.producer.return_value = producer
    config = Mock()

    async def inner_test(expected_producer: Mock) -> None:
        """
        inner test to allow async calls
        :return: None
        """
        producer = await setup_producer(config)
        assert producer == expected_producer

    asyncio.run(inner_test(producer))
    memphis.producer.assert_called_once_with(station_name=config.station_name, producer_name=config.producer_name)
    memphis.connect.assert_called_once_with(host=config.host, username=config.username, password=config.password)


@patch("file_watcher.file_watcher.setup_producer")
@patch("file_watcher.file_watcher.setup_watcher")
@patch("file_watcher.file_watcher.watch")
@patch("file_watcher.file_watcher.SimpleQueue")
@patch("file_watcher.file_watcher.load_config")
def test_main(mock_load_config, mock_queue_class, mock_watch, mock_setup_watcher, mock_setup_producer):
    """
    Test main calls
    :return: None
    """
    producer = Mock()
    queue = Mock()
    config = Mock()
    mock_queue_class.return_value = queue
    mock_setup_producer.return_value = producer
    mock_load_config.return_value = config

    asyncio.run(main())

    mock_setup_producer.assert_called_once_with(config)
    mock_setup_watcher.assert_called_once_with(queue, config)
    mock_watch.assert_called_once_with(queue, producer)


def test_load_config():
    """
    Test config loading
    :return: None
    """
    os.environ["MEMPHIS_HOST"] = "localhost_test"
    os.environ["MEMPHIS_USER"] = "user_test"
    os.environ["MEMPHIS_PASS"] = "password_test"
    os.environ["MEMPHIS_STATION"] = "station_test"
    os.environ["MEMPHIS_PRODUCER_NAME"] = "producername_test"
    os.environ["WATCH_DIR"] = "./file_watcher_test"

    config = load_config()

    assert config.host == "localhost_test"
    assert config.username == "user_test"
    assert config.password == "password_test"
    assert config.station_name == "station_test"
    assert config.producer_name == "producername_test"
    assert config.watch_dir == "./file_watcher_test"


def test_load_config_defaults():
    """
    Test default config loading
    :return: None
    """
    # Clear environment variables
    os.environ.pop("MEMPHIS_HOST", None)
    os.environ.pop("MEMPHIS_USER", None)
    os.environ.pop("MEMPHIS_PASS", None)
    os.environ.pop("MEMPHIS_STATION", None)
    os.environ.pop("MEMPHIS_PRODUCER_NAME", None)
    os.environ.pop("WATCH_DIR", None)

    config = load_config()

    assert config.host == "localhost"
    assert config.username == "user"
    assert config.password == "password"
    assert config.station_name == "station"
    assert config.producer_name == "producername"
    assert config.watch_dir == "./file_watcher"
