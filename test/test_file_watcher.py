"""
Main test cases
"""
import asyncio
from queue import SimpleQueue
from unittest.mock import patch, Mock, AsyncMock

import pytest

from file_watcher.file_watcher import watch, setup_watcher, setup_producer, main


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
    mock_handler = Mock()
    mock_handler_class.return_value = mock_handler
    mock_observer = Mock()
    mock_observer_class.return_value = mock_observer
    setup_watcher(queue)
    mock_handler_class.assert_called_once_with(queue)
    mock_observer_class.assert_called_once()
    mock_observer.schedule.assert_called_once_with(mock_handler, "./file_watcher")
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

    async def inner_test(expected_producer: Mock) -> None:
        """
        inner test to allow async calls
        :return: None
        """
        producer = await setup_producer()
        assert producer == expected_producer

    asyncio.run(inner_test(producer))
    memphis.producer.assert_called_once_with(station_name="rundetection", producer_name="producername")
    memphis.connect.assert_called_once_with(host="localhost", username="rundetection", password="password")


@patch("file_watcher.file_watcher.setup_producer")
@patch("file_watcher.file_watcher.setup_watcher")
@patch("file_watcher.file_watcher.watch")
@patch("file_watcher.file_watcher.SimpleQueue")
def test_main(mock_queue_class, mock_watch, mock_setup_watcher, mock_setup_producer):
    """
    Test main calls
    :return: None
    """
    producer = Mock()
    queue = Mock()
    mock_queue_class.return_value = queue
    mock_setup_producer.return_value = producer

    asyncio.run(main())

    mock_setup_watcher.assert_called_once_with(queue)
    mock_watch.assert_called_once_with(queue, producer)
