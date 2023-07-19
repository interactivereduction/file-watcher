"""
Main module
"""
import asyncio
import os
from dataclasses import dataclass
from pathlib import Path

from memphis import Memphis  # type: ignore
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
        os.environ.get("DB_PASSWORD", "admin")
    )


class FileWatcher:
    def __init__(self, config):
        self.config = config
        self.memphis = Memphis()

    async def _init(self):
        await self.connect_to_broker()
        self.producer = await self.setup_producer()

    async def connect_to_broker(self):
        logger.info("Connecting to memphis at host: %s", self.config.host)
        await self.memphis.connect(host=self.config.host, username=self.config.username,
                                   password=self.config.password, timeout_ms=30000)
        logger.info("Connected to memphis")

    async def setup_producer(self) -> Producer:
        """
        Asynchronously setup and return a memphis producer
        :return: The memphis producer
        """
        logger.info("Creating producer: %s at station: %s", self.config.producer_name, self.config.station_name)
        return await self.memphis.producer(station_name=self.config.station_name,
                                           producer_name=self.config.producer_name,
                                           generate_random_suffix=True)

    async def on_event(self, path: Path) -> None:
        str_path = str(path)
        if path.is_dir():
            logger.info("Skipping directory creation for: %s", str_path)
        else:
            if not self.memphis.is_connected():
                logger.info("Memphis is not connected...")
                await self.connect_to_broker()
            logger.info("Producing message: %s", str_path)
            await self.producer.produce(bytearray(str_path, "utf-8"))

    async def start_watching(self) -> None:
        """
        Start the PollingObserver with the queue based event handler and the given queue
        :param queue: The queue for the event handler to use
        :return: None
        """
        async def _event_occurred(path_to_add):
            await self.on_event(path_to_add)
        last_run_detector = \
            await create_last_run_detector(self.config.watch_dir, self.config.instrument_folder, _event_occurred,
                                           run_file_prefix=self.config.run_file_prefix, db_ip=self.config.db_ip,
                                           db_username=self.config.db_username, db_password=self.config.db_password)
        try:
            await last_run_detector.watch_for_new_runs()
        except Exception as exception:
            logger.info("File observer fell over watching because of the following exception:")
            logger.exception(exception)


async def start() -> None:
    """
    Create the file watcher and start watching for changes
    :return: None
    """
    config = load_config()
    file_watcher = FileWatcher(config)
    await file_watcher._init()
    await file_watcher.start_watching()


def main():
    """Main function"""
    asyncio.run(start())


if __name__ == "__main__":
    main()
