"""
Module containing custom event handlers to be used by the watchdog library
"""
from __future__ import annotations

import logging
from queue import SimpleQueue

from watchdog.events import FileSystemEventHandler, FileCreatedEvent

logger = logging.getLogger(__name__)


class QueueBasedEventHandler(FileSystemEventHandler):
    """
    QueueBasedEventHandler puts the source path of created files onto the given SimpleQueue[str]
    """

    def __init__(self, queue: SimpleQueue[str]):
        self._queue = queue

    def on_created(self, event: FileCreatedEvent) -> None:
        """
        Automatically called on FileCreatedEvent, puts a created file on the queue, ignoring dirs.
        :param event: The triggered FileCreatedEvent
        :return: None
        """
        logger.info("Creation event triggered: %s", event.src_path)
        if event.is_directory:
            logger.info("Skipping directory creation")
            pass
        else:
            logger.info("Adding to %s to queue", event.src_path)
            self._queue.put(event.src_path)
