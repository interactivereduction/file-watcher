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


async def setup_producer(config: Config) -> Producer:
    """
    Asynchronously setup and return a memphis producer
    :return: The memphis producer
    """
    memphis = Memphis()
    logger.info("Connecting to memphis at host: %s", config.host)
    await memphis.connect(host=config.host, username=config.username, password=config.password)
    logger.info("Connected")
    logger.info("Creating producer: %s  at station: %s", config.producer_name, config.station_name)
    return await memphis.producer(station_name=config.station_name, producer_name=config.producer_name,
                                  generate_random_suffix=True)


async def on_event(producer: Producer, path: Path) -> None:
    str_path = str(path)
    if path.is_dir():
        logger.info("Skipping directory creation for: %s", str_path)
    else:
        logger.info("Producing message: %s", str_path)
        await producer.produce(bytearray(str_path, "utf-8"))


async def start_watching(producer: Producer, config: Config) -> None:
    """
    Start the PollingObserver with the queue based event handler and the given queue
    :param queue: The queue for the event handler to use
    :return: None
    """
    async def _event_occured(path_to_add):
        await on_event(producer, path_to_add)
    file_observer = \
        await create_last_run_detector(config.watch_dir,  config.instrument_folder, _event_occured,
                                       run_file_prefix=config.run_file_prefix, db_ip=config.db_ip,
                                       db_username=config.db_username, db_password=config.db_password)
    await file_observer.watch_for_new_runs()


async def start() -> None:
    """
    Start the producer, file watcher and creating the queue
    :return: None
    """
    config = load_config()
    producer = await setup_producer(config)
    await start_watching(producer, config)


def main():
    """Main function"""
    asyncio.run(start())


if __name__ == "__main__":
    main()
