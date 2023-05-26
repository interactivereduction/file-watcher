from queue import SimpleQueue

import pytest as pytest
from watchdog.events import FileCreatedEvent

from file_watcher.event_handlers import QueueBasedEventHandler


@pytest.fixture
def queue() -> SimpleQueue[str]:
    return SimpleQueue()


@pytest.fixture
def event_handler(queue):
    return QueueBasedEventHandler(queue)


def test_on_create_skips_directory(event_handler, queue):
    event = FileCreatedEvent("/foo/")
    event.is_directory = True
    event_handler.on_created(event)
    assert queue.empty()


def test_on_create_event_adds_file_to_queue(event_handler, queue):
    event = FileCreatedEvent("/foo/bar.nxs")
    event_handler.on_created(event)

    assert queue.get() == "/foo/bar.nxs"
