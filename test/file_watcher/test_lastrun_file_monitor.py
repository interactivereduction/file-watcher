import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock
from unittest.mock import call

import pytest

from file_watcher.lastrun_file_monitor import create_last_run_detector, LastRunDetector
from test.file_watcher.utils import AwaitableNonAsyncMagicMock


class LastRunFileMonitorTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.create_instrument_files()
        self.callback = AwaitableNonAsyncMagicMock()
        self.run_file_prefix = mock.MagicMock()
        self.db_ip = mock.MagicMock()
        self.db_username = mock.MagicMock()
        self.db_password = mock.MagicMock()

    def create_instrument_files(self):
        # Create this instrument
        self.archive_path = Path("/tmp/")
        with tempfile.TemporaryDirectory() as tmpdirname:
            self.path = Path(tmpdirname)
        self.instrument = self.path.name
        self.path = self.path / "Instrument" / "logs"
        self.path.mkdir(parents=True, exist_ok=True)
        self.path = self.path / "lastrun.txt"
        with open(self.path, "+w") as file:
            file.write(f"{self.instrument} 0001 0")

        # Ensure wish exists
        wish_path = self.archive_path / "NDXWISH" / "instrument" / "data" / "cycle_23_2"
        wish_path.mkdir(parents=True, exist_ok=True)

    @pytest.mark.asyncio
    async def test_create_last_run_detector(self):
        with mock.patch("file_watcher.lastrun_file_monitor.LastRunDetector", new=AwaitableNonAsyncMagicMock()) as lrd:
            await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )

            lrd.assert_called_once_with(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            lrd.return_value._init.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_get_latest_run_from_db(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as db_updater:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )

            self.lrd.get_latest_run_from_db()

            db_updater.return_value.get_latest_run.assert_called_with(self.instrument[3:])
            self.assertEqual(db_updater.return_value.get_latest_run.call_count, 2)

    @pytest.mark.asyncio
    async def test_watch_for_new_runs_checks_for_latest_cycle_after_6_hours(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            now = datetime.now()
            self.lrd.last_cycle_folder_check = datetime.now() - timedelta(seconds=21601)  # Set to over 6 hours ago
            self.lrd.get_latest_cycle = mock.MagicMock()

            self.assertEqual(self.lrd.get_latest_cycle.call_count, 0)

            await self.lrd.watch_for_new_runs(run_once=True)

            self.assertEqual(self.lrd.get_latest_cycle.call_count, 1)
            self.assertGreater(self.lrd.last_cycle_folder_check, now)

    @pytest.mark.asyncio
    async def test_watch_for_new_runs_checks_contents_of_last_run_file(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            self.lrd.get_last_run_from_file = mock.MagicMock()

            await self.lrd.watch_for_new_runs(run_once=True)

            self.lrd.get_last_run_from_file.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_watch_for_new_runs_handles_exceptions_from_get_last_run_from_file(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            exception = RuntimeError("EXCEPTIONS!")

            def raise_exception():
                raise exception

            self.lrd.get_last_run_from_file = mock.MagicMock(side_effect=raise_exception)

            with mock.patch("file_watcher.lastrun_file_monitor.logger") as logger:
                await self.lrd.watch_for_new_runs(run_once=True)

                self.lrd.get_last_run_from_file.assert_called_once_with()

                logger.exception.assert_called_once_with(exception)

    @pytest.mark.asyncio
    async def test_latest_run_is_new(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            self.lrd.get_last_run_from_file = mock.MagicMock(return_value="0002")
            self.lrd.new_run_detected = AwaitableNonAsyncMagicMock()

            await self.lrd.watch_for_new_runs(run_once=True)

            self.lrd.get_last_run_from_file.assert_called_once_with()
            self.lrd.new_run_detected.assert_called_once_with("0002")

    @pytest.mark.asyncio
    async def test_latest_run_is_new_and_more_than_one_file(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            self.lrd.get_last_run_from_file = mock.MagicMock(return_value="0003")
            self.lrd.recover_lost_runs = AwaitableNonAsyncMagicMock()

            await self.lrd.watch_for_new_runs(run_once=True)

            self.lrd.get_last_run_from_file.assert_called_once_with()
            self.lrd.recover_lost_runs.assert_called_with("0001", "0003")

    @pytest.mark.asyncio
    async def test_latest_run_is_not_new(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            self.lrd.get_last_run_from_file = mock.MagicMock(return_value="0001")
            self.lrd.recover_lost_runs = AwaitableNonAsyncMagicMock()
            self.lrd.new_run_detected = AwaitableNonAsyncMagicMock()

            await self.lrd.watch_for_new_runs(run_once=True)

            self.lrd.get_last_run_from_file.assert_called_once_with()
            self.lrd.recover_lost_runs.assert_not_called()
            self.lrd.new_run_detected.assert_not_called()

    @pytest.mark.asyncio
    async def test_generate_run_path_functions_as_expected(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            self.lrd.lastest_cycle = "cycle_23_2"
            self.lrd.run_file_prefix = "TMP"
            expected_path = self.path.parent.parent / "data" / "cycle_23_2"
            expected_path.mkdir(parents=True, exist_ok=True)
            expected_path = expected_path / f"{self.lrd.run_file_prefix}0001.nxs"
            with open(expected_path, "+w") as file:
                file.write(f"HELLO!")

            returned_path = self.lrd.generate_run_path("0001")

            self.assertEqual(expected_path, returned_path)

    @pytest.mark.asyncio
    async def test_generate_run_path_handles_the_file_not_existing(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )

            def raise_exception():
                raise Exception()

            self.lrd.find_file_in_instruments_data_folder = mock.MagicMock(side_effect=raise_exception)

            with self.assertRaises(FileNotFoundError):
                self.lrd.generate_run_path("0001")

    @pytest.mark.asyncio
    async def test_generate_run_path_handles_the_file_not_existing_where_expected_but_in_another_folder(
        self,
    ):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            expected_path = mock.MagicMock()
            self.lrd.find_file_in_instruments_data_folder = mock.MagicMock(return_value=expected_path)

            returned_path = self.lrd.generate_run_path("0001")

            self.lrd.find_file_in_instruments_data_folder.assert_called_once_with("0001")
            self.assertEqual(expected_path, returned_path)

    @pytest.mark.asyncio
    async def test_new_run_detected_handles_just_run_number(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            self.lrd.update_db_with_latest_run = mock.MagicMock()
            run_path = mock.MagicMock()
            self.lrd.generate_run_path = mock.MagicMock(return_value=run_path)

            await self.lrd.new_run_detected("0001")

            self.lrd.update_db_with_latest_run.assert_called_once_with("0001")

    @pytest.mark.asyncio
    async def test_new_run_detected_handles_file_not_found_by_generate_run_path(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            exception = FileNotFoundError("FILE NOT FOUND!")

            def raise_file_not_found(_):
                raise exception

            self.lrd.generate_run_path = mock.MagicMock(side_effect=raise_file_not_found)

            with mock.patch("file_watcher.lastrun_file_monitor.logger") as logger:
                await self.lrd.new_run_detected(run_path=None, run_number="0001")

                logger.exception.assert_called_once_with(exception)

            self.lrd.generate_run_path.assert_called_once_with("0001")

    @pytest.mark.asyncio
    async def test_get_last_run_from_file_raises_when_file_formatted_poorly(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            with tempfile.NamedTemporaryFile(delete=False) as fp:
                fp.write(b"Hello world!")
                path = Path(fp.name)

            self.lrd.last_run_file = path

            self.assertRaises(RuntimeError, self.lrd.get_last_run_from_file)

    @pytest.mark.asyncio
    async def test_recover_lost_runs_finds_runs_that_were_lost(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            self.lrd.generate_run_path = mock.MagicMock(return_value=Path("/run/path/NDXMARI/MAR001"))

            await self.lrd.recover_lost_runs("0001", "0003")

            self.assertEqual(self.lrd.generate_run_path.call_args_list, [call("0002"), call("0003")])
            self.assertEqual(self.lrd.generate_run_path.call_count, 2)

    @pytest.mark.asyncio
    async def test_recover_lost_runs_handles_file_not_found_twice(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            exception = FileNotFoundError("FILE NOT FOUND!")

            def raise_file_not_found(_):
                raise exception

            self.lrd.generate_run_path = mock.MagicMock(side_effect=raise_file_not_found)

            with mock.patch("file_watcher.lastrun_file_monitor.logger") as logger:
                await self.lrd.recover_lost_runs("0001", "0003")
                logger.exception.assert_called_with(exception)
                self.assertEqual(logger.exception.call_count, 2)

            self.assertEqual(
                self.lrd.generate_run_path.call_args_list, [call("0002"), call("002"), call("0003"), call("003")]
            )
            self.assertEqual(self.lrd.generate_run_path.call_count, 4)

    @pytest.mark.asyncio
    async def test_update_db_with_latest_run_sends_instrument_minus_NDX(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            self.lrd.instrument = "NDXMARI"
            self.lrd.db.update_latest_run = mock.MagicMock()

            self.lrd.update_db_with_latest_run("0001")

            self.lrd.db.update_latest_run.assert_called_once_with("MARI", "0001")

    @pytest.mark.asyncio
    async def test_find_file_in_instruments_data_folder_finds_file_in_instrument_data_folder(
        self,
    ):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            run_number = "0001"
            self.lrd.archive_path = mock.MagicMock()
            instrument_dir = self.lrd.archive_path.joinpath.return_value.joinpath.return_value
            instrument_dir.rglob = mock.MagicMock(return_value=["banana"])

            return_value = self.lrd.find_file_in_instruments_data_folder(run_number)

            self.lrd.archive_path.joinpath.assert_called_once_with(self.lrd.instrument)
            self.lrd.archive_path.joinpath.return_value.joinpath.assert_called_once_with("Instrument/data")
            instrument_dir.rglob.assert_called_once_with(f"cycle_??_?/*{run_number}.nxs")

            self.assertEqual(return_value, "banana")

    @pytest.mark.asyncio
    async def test_get_latest_cycle_handles_lack_of_cycles_in_archive(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            with tempfile.TemporaryDirectory() as tmpdirname:
                path = Path(tmpdirname)
                self.lrd.archive_path = path
                path = path / "NDXWISH" / "instrument" / "data"
                path.mkdir(parents=True, exist_ok=True)

                self.assertRaises(FileNotFoundError, self.lrd.get_latest_cycle)

    @pytest.mark.asyncio
    async def test_get_latest_cycle_finds_latest_cycle(self):
        with mock.patch("file_watcher.lastrun_file_monitor.DBUpdater", new=AwaitableNonAsyncMagicMock()) as _:
            self.lrd = await create_last_run_detector(
                self.archive_path,
                self.instrument,
                self.callback,
                self.run_file_prefix,
                self.db_ip,
                self.db_username,
                self.db_password,
            )
            with tempfile.TemporaryDirectory() as tmpdirname:
                path = Path(tmpdirname)
                self.lrd.archive_path = path
                path = path / "NDXWISH" / "instrument" / "data" / "cycle_25_2"
                path.mkdir(parents=True, exist_ok=True)

                latest_cycle = self.lrd.get_latest_cycle()

                self.assertEqual(latest_cycle, "cycle_25_2")
