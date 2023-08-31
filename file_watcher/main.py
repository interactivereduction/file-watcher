# pylint: disable=too-many-instance-attributes
"""
Main module
"""
import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Union

from memphis import Memphis, MemphisError  # type: ignore
from memphis.producer import Producer  # type: ignore

from file_watcher.lastrun_file_monitor import create_last_run_detector
from file_watcher.utils import logger


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
    watch_dir: Path
    run_file_prefix: str
    instrument_folder: str
    db_ip: str
    db_username: str
    db_password: str


def load_config() -> Config:
    """
    Load config values from env vars or get defaults and return the config object
    :return: Config
    """
    return Config(
        os.environ.get("MEMPHIS_HOST", "localhost"),
        os.environ.get("MEMPHIS_USER", "root"),
        os.environ.get("MEMPHIS_PASS", "memphis"),
        os.environ.get("MEMPHIS_STATION", "station"),
        os.environ.get("MEMPHIS_PRODUCER_NAME", "producername"),
        Path(os.environ.get("WATCH_DIR", "/archive")),
        os.environ.get("FILE_PREFIX", "MAR"),
        os.environ.get("INSTRUMENT_FOLDER", "NDXMARI"),
        os.environ.get("DB_IP", "localhost"),
        os.environ.get("DB_USERNAME", "admin"),
        os.environ.get("DB_PASSWORD", "admin"),
    )


class FileWatcher:
    """
    The FileWatcher is responsible for owning and running the latest run file detector and then ensuring that Memphis
    received those messages.
    """

    def __init__(self, config: Config):
        self.config = config
        self.memphis = Memphis()

    async def _init(self) -> None:
        """
        This function needs to be called before any other function and is the equivalent of a setup, it has to be
        done outside __init__ because it is an Async function, and async functionality cannot be completed inside
        __init__.
        """
        await self.connect_to_broker()
        self.producer = await self.setup_producer()  # pylint: disable=attribute-defined-outside-init

    async def connect_to_broker(self) -> None:
        """
        A function to connect to the memphis broker can be called multiple times in a row without issue
        """
        logger.info("Connecting to memphis at host: %s", self.config.host)
        await self.memphis.connect(
            host=self.config.host, username=self.config.username, password=self.config.password, timeout_ms=30000
        )
        logger.info("Connected to memphis")

    async def setup_producer(self) -> Producer:
        """
        Asynchronously setup and return a memphis producer
        :return: The memphis producer
        """
        logger.info("Creating producer: %s at station: %s", self.config.producer_name, self.config.station_name)
        return await self.memphis.producer(
            station_name=self.config.station_name, producer_name=self.config.producer_name, generate_random_suffix=True
        )

    async def on_event(self, path: Path) -> None:
        """
        The function to be called when you have a new file detected, it will talk to memphis and produce a new message
        :param path: The path that should be the contents of the Memphis message
        """
        str_path = str(path)
        if path.is_dir():
            logger.info("Skipping directory creation for: %s", str_path)
        else:
            if not self.memphis.is_connected():
                logger.info("Memphis is not connected...")
                await self.connect_to_broker()
            logger.info("Producing message: %s", str_path)
            try:
                await self.producer.produce(bytearray(str_path, "utf-8"))
            except MemphisError:
                # Assume an error connecting to memphis, connect again and resubmit
                await self.connect_to_broker()
                await self.producer.produce(bytearray(str_path, "utf-8"))

    async def start_watching(self) -> None:
        """
        Start the PollingObserver with the queue based event handler and the given queue
        :return: None
        """

        async def _event_occurred(path_to_add: Union[Path, None]) -> None:
            if path_to_add is not None:
                await self.on_event(path_to_add)

        last_run_detector = await create_last_run_detector(
            self.config.watch_dir,
            self.config.instrument_folder,
            _event_occurred,
            run_file_prefix=self.config.run_file_prefix,
            db_ip=self.config.db_ip,
            db_username=self.config.db_username,
            db_password=self.config.db_password,
        )

        try:
            await last_run_detector.watch_for_new_runs()
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.info("File observer fell over watching because of the following exception:")
            logger.exception(exception)


async def start() -> None:
    """
    Create the file watcher and start watching for changes
    :return: None
    """
    config = load_config()
    file_watcher = FileWatcher(config)
    await file_watcher._init()  # pylint: disable=protected-access
    await file_watcher.start_watching()


def main() -> None:
    """Main function"""
    asyncio.run(start())


if __name__ == "__main__":
    main()
