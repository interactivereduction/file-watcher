import datetime
import os
from pathlib import Path
from time import sleep
from typing import Callable

from file_watcher.database.db_updater import DBUpdater
from file_watcher.utils import logger


async def create_last_run_detector(archive_path: Path, instrument: str, callback: Callable, run_file_prefix: str,
                             db_ip: str, db_username: str, db_password: str):
    lrd = LastRunDetector(archive_path, instrument, callback, run_file_prefix, db_ip, db_username, db_password)
    await lrd._init()
    return lrd


class LastRunDetector:
    def __init__(self, archive_path: Path, instrument: str, async_callback: Callable, run_file_prefix: str, db_ip: str,
                 db_username: str, db_password: str):
        self.instrument = instrument
        self.run_file_prefix = run_file_prefix
        self.async_callback = async_callback
        self.archive_path = archive_path
        self.last_run_file = archive_path.joinpath(instrument).joinpath("Instrument/logs/lastrun.txt")
        self.last_recorded_run_from_file = self.get_last_run_from_file()
        logger.info(f"Last run in lastrun.txt for instrument {self.instrument} is: {self.last_recorded_run_from_file}")
        self.last_cycle_folder_check = datetime.datetime.now()
        self.latest_cycle = self.get_latest_cycle()

        # Database setup and checks if runs missed then recovery
        self.db = DBUpdater(ip=db_ip, username=db_username, password=db_password)
        self.latest_known_run_from_db = self.get_latest_run_from_db()
        logger.info(f"Last run in DB is: {self.latest_known_run_from_db}")
        if self.latest_known_run_from_db is None:
            logger.info(f"Adding latest run to DB as there is no data: {self.last_recorded_run_from_file}")
            self.update_db_with_latest_run(self.last_recorded_run_from_file)
            self.latest_known_run_from_db = self.last_recorded_run_from_file

    async def _init(self):
        if int(self.latest_known_run_from_db) < int(self.last_recorded_run_from_file):
            logger.info(f"Recovering lost runs between {self.latest_known_run_from_db} and "
                        f"{self.last_recorded_run_from_file}")
            await self.recover_lost_runs(self.latest_known_run_from_db, self.last_recorded_run_from_file)

    def get_latest_run_from_db(self) -> str:
        # This likely contains NDX<INSTNAME> so remove the NDX and go for it with the DB
        actual_instrument = self.instrument[3:]
        return self.db.get_latest_run(actual_instrument)

    async def watch_for_new_runs(self):
        logger.info("Starting watcher...")
        while True:
            time_between_cycle_folder_checks = datetime.datetime.now() - self.last_cycle_folder_check
            # If it's been 6 hours do another check for the latest folder
            if time_between_cycle_folder_checks.seconds > 21600:
                self.latest_cycle = self.get_latest_cycle()

            run_in_file = self.get_last_run_from_file()
            if run_in_file != self.last_recorded_run_from_file:
                logger.info(f"New run detected: {run_in_file}")
                # If difference > 1 then try to recover potentially missed runs:
                if int(run_in_file) - int(self.last_recorded_run_from_file) > 1:
                    await self.recover_lost_runs(self.last_recorded_run_from_file, run_in_file)
                else:
                    await self.new_run_detected(run_in_file)

            sleep(0.1)

    def generate_run_path(self, run_number: str) -> Path:
        path = self.archive_path.joinpath(self.instrument).joinpath("Instrument/data").joinpath(self.latest_cycle)\
            .joinpath(self.run_file_prefix + run_number + ".nxs")
        if not path.exists():
            try:
                path = self.find_file_in_instruments_data_folder(run_number)
            except:
                raise FileNotFoundError(f"This run number doesn't have a file: {run_number}")
        return path

    async def new_run_detected(self, run_number: str):
        await self.async_callback(self.generate_run_path(run_number))
        self.update_db_with_latest_run(run_number)
        self.last_recorded_run_from_file = run_number

    def get_last_run_from_file(self):
        with open(self.last_run_file, mode='r', encoding="utf-8") as last_run:
            line_parts = last_run.readline().split()
            if len(line_parts) != 3:
                raise RuntimeError(f"Unexpected last run file format for '{self.last_run_file}'")
        return line_parts[1]

    async def recover_lost_runs(self, earlier_run, later_run):
        """
        The aim is to send all the runs that have not been sent, in between the two passed run numbers, it will also
        submit the value for later_run
        """
        for run in range(int(earlier_run) + 1, int(later_run) + 1):
            # If file exists new run detected
            run_path = self.generate_run_path(str(run))
            if run_path.exists():
                await self.new_run_detected(str(run))
            else:
                logger.log(f"Run cannot be recovered as the file/path does not exist: {run_path}")

    def update_db_with_latest_run(self, run_number):
        # This likely contains NDX<INSTNAME> so remove the NDX and go for it with the DB
        actual_instrument = self.instrument[3:]
        self.db.update_latest_run(actual_instrument, run_number)

    def find_file_in_instruments_data_folder(self, run_number: str) -> Path:
        """
        Slow but guaranteed to find the file if it exists.
        """
        instrument_dir = self.archive_path.joinpath(self.instrument).joinpath("Instrument/data")
        return list(instrument_dir.rglob(f"cycle_??_?/*{run_number}.nxs"))[0]

    def get_latest_cycle(self) -> str:
        """
        Gets the latest cycle, uses NDXWISH as the bases of it, as it is a TS2 instrument and allows for
        significantly reduced complications vs TS1 instruments who collected data in cycles_98_1 and so on (centuries
        and all that being rather complicated for a machine to understand without appropriate context).
        """
        logger.info("Finding latest cycle...")
        # Use WISH (or any other TS2 instrument as their data started in 2008 and avoids the 98/99 issue of TS1
        # instruments) to determine which is the most recent cycle.
        all_cycles = os.listdir(f"{self.archive_path}/NDXWISH/instrument/data/")
        all_cycles.sort()
        most_recent_cycle = all_cycles[-1]
        if most_recent_cycle is None:
            raise FileNotFoundError(f"No cycles present in archive path: {self.archive_path}")
        logger.info(f"Latest cycle found: {most_recent_cycle}")
        return most_recent_cycle
