"""
Main module
"""
import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from queue import SimpleQueue

from memphis import Memphis  # type: ignore
from memphis.producer import Producer  # type: ignore
from watchdog.observers.polling import PollingObserver

from file_watcher.event_handlers import QueueBasedEventHandler

stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    """
    Config for watcher
    """

    host: str
    username: str
    password: str
    station_name: str
    producer_name: str
    watch_dir: str


def load_config() -> Config:
    return Config(
        os.environ.get("MEMPHIS_HOST", "localhost"),
        os.environ.get("MEMPHIS_USER", "user"),
        os.environ.get("MEMPHIS_PASS", "password"),
        os.environ.get("MEMPHIS_STATION", "station"),
        os.environ.get("MEMPHIS_PRODUCER_NAME", "producername"),
        os.environ.get("WATCH_DIR", "./file_watcher"),
    )


async def setup_producer(config: Config) -> Producer:
    """
    Asynchronously setup and return a memphis producer
    :return: The memphis producer
    """
    memphis = Memphis()
    await memphis.connect(host=config.host, username=config.username, password=config.password)
    return await memphis.producer(station_name=config.station_name, producer_name=config.producer_name)


def setup_watcher(queue: SimpleQueue[str], config: Config) -> None:
    """
    Start the PollingObserver with the queue based event handler and the given queue
    :param queue: The queue for the event handler to use
    :return: None
    """
    event_handler = QueueBasedEventHandler(queue)
    observer = PollingObserver()  # type: ignore
    observer.schedule(event_handler, config.watch_dir)  # type: ignore
    observer.start()  # type: ignore


async def watch(queue: SimpleQueue[str], producer: Producer) -> None:
    """
    Loop with a 400 ms delay to check the queue for new files and send to the station if found
    :param queue: The queue
    :param producer: The memphis producer
    :return: None
    """
    while True:
        if not queue.empty():
            await producer.produce(queue.get())
        await asyncio.sleep(0.4)


async def main() -> None:
    """
    Main Entrypoint starting the producer, file watcher and creating the queue
    :return: None
    """
    config = load_config()
    producer = await setup_producer(config)
    queue: SimpleQueue[str] = SimpleQueue()
    setup_watcher(queue, config)
    await watch(queue, producer)


if __name__ == "__main__":
    asyncio.run(main())
