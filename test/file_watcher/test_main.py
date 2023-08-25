import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from memphis import MemphisError

from file_watcher.main import load_config, FileWatcher, start, main
from test.file_watcher.utils import AwaitableNonAsyncMagicMock


class MainTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = load_config()

    async def async_raises(self, exception, coro):
        with self.assertRaises(exception):
            await coro

    def test_load_config_defaults(self):
        config = load_config()

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.username, "root")
        self.assertEqual(config.password, "memphis")
        self.assertEqual(config.station_name, "station")
        self.assertEqual(config.producer_name, "producername")
        self.assertEqual(config.watch_dir, Path("/archive"))
        self.assertEqual(config.run_file_prefix, "MAR")
        self.assertEqual(config.instrument_folder, "NDXMARI")
        self.assertEqual(config.db_ip, "localhost")
        self.assertEqual(config.db_username, "admin")
        self.assertEqual(config.db_password, "admin")

    def test_load_config_environ_vars(self):
        host = str(mock.MagicMock())
        username = str(mock.MagicMock())
        password = str(mock.MagicMock())
        station_name = str(mock.MagicMock())
        producer_name = str(mock.MagicMock())
        watch_dir = "/" + str(mock.MagicMock())
        run_file_prefix = str(mock.MagicMock())
        instrument_folder = str(mock.MagicMock())
        db_ip = str(mock.MagicMock())
        db_username = str(mock.MagicMock())
        db_password = str(mock.MagicMock())

        os.environ["MEMPHIS_HOST"] = host
        os.environ["MEMPHIS_USER"] = username
        os.environ["MEMPHIS_PASS"] = password
        os.environ["MEMPHIS_STATION"] = station_name
        os.environ["MEMPHIS_PRODUCER_NAME"] = producer_name
        os.environ["WATCH_DIR"] = watch_dir
        os.environ["FILE_PREFIX"] = run_file_prefix
        os.environ["INSTRUMENT_FOLDER"] = instrument_folder
        os.environ["DB_IP"] = db_ip
        os.environ["DB_USERNAME"] = db_username
        os.environ["DB_PASSWORD"] = db_password

        config = load_config()

        self.assertEqual(config.host, host)
        self.assertEqual(config.username, username)
        self.assertEqual(config.password, password)
        self.assertEqual(config.station_name, station_name)
        self.assertEqual(config.producer_name, producer_name)
        self.assertEqual(config.watch_dir, Path(watch_dir))
        self.assertEqual(config.run_file_prefix, run_file_prefix)
        self.assertEqual(config.instrument_folder, instrument_folder)
        self.assertEqual(config.db_ip, db_ip)
        self.assertEqual(config.db_username, db_username)
        self.assertEqual(config.db_password, db_password)

    @pytest.mark.asyncio
    async def test_file_watcher_connect_to_broker_connects_using_config(self):
        with mock.patch("file_watcher.main.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
            self.file_watcher = FileWatcher(self.config)
            await self.file_watcher._init()

            await self.file_watcher.connect_to_broker()

            self.file_watcher.memphis.connect.assert_called_with(host=self.config.host,
                                                                 username=self.config.username,
                                                                 password=self.config.password,
                                                                 timeout_ms=30000)
            self.assertEqual(self.file_watcher.memphis.connect.call_count, 2)

    @pytest.mark.asyncio
    async def test_file_watcher_setup_producer_creates_producer(self):
        with mock.patch("file_watcher.main.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
            self.file_watcher = FileWatcher(self.config)
            await self.file_watcher._init()
            self.file_watcher.producer = AwaitableNonAsyncMagicMock()

            await self.file_watcher.setup_producer()

            self.file_watcher.memphis.producer.assert_called_with(station_name=self.config.station_name,
                                                                  producer_name=self.config.producer_name,
                                                                  generate_random_suffix=True)
            self.assertEqual(self.file_watcher.memphis.producer.call_count, 2)

    @pytest.mark.asyncio
    async def test_file_watcher_on_event_if_not_connected_connects(self):
        with mock.patch("file_watcher.main.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
            self.file_watcher = FileWatcher(self.config)
            await self.file_watcher._init()
            self.file_watcher.producer = AwaitableNonAsyncMagicMock()
            self.file_watcher.connect_to_broker = AwaitableNonAsyncMagicMock()
            self.file_watcher.memphis.is_connected = mock.MagicMock(return_value=False)

            with tempfile.NamedTemporaryFile(delete=False) as fp:
                fp.write(b'Hello world!')
                path = Path(fp.name)

            await self.file_watcher.on_event(path)

            self.file_watcher.connect_to_broker.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_file_watcher_on_event_if_memphis_network_pipe_broken_reconnect_and_try_again(self):
        with mock.patch("file_watcher.main.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
            self.file_watcher = FileWatcher(self.config)
            await self.file_watcher._init()

            def raise_memphis_error(_):
                raise MemphisError("")

            produce = AwaitableNonAsyncMagicMock(side_effect=raise_memphis_error)
            self.file_watcher.producer = AwaitableNonAsyncMagicMock()
            self.file_watcher.producer.produce = produce
            self.file_watcher.memphis.is_connected = mock.MagicMock(return_value=True)
            self.file_watcher.connect_to_broker = AwaitableNonAsyncMagicMock()

            with tempfile.NamedTemporaryFile(delete=False) as fp:
                fp.write(b'Hello world!')
                path = Path(fp.name)

            with self.assertRaises(MemphisError):
                await self.file_watcher.on_event(path)
            self.file_watcher.connect_to_broker.assert_called_once_with()
            self.file_watcher.producer.produce.assert_called_with(bytearray(str(path), "utf-8"))
            self.assertEqual(self.file_watcher.producer.produce.call_count, 2)

    @pytest.mark.asyncio
    async def test_file_watcher_on_event_produces_message(self):
        with mock.patch("file_watcher.main.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
            self.file_watcher = FileWatcher(self.config)
            await self.file_watcher._init()
            producer = AwaitableNonAsyncMagicMock()
            self.file_watcher.producer = producer
            self.file_watcher.connect_to_broker = mock.MagicMock()
            self.file_watcher.is_connected = mock.MagicMock(return_value=True)

            with tempfile.NamedTemporaryFile(delete=False) as fp:
                fp.write(b'Hello world!')
                path = Path(fp.name)

            await self.file_watcher.on_event(path)
            producer.produce.assert_called_once_with(bytearray(str(path), "utf-8"))

    @pytest.mark.asyncio
    async def test_file_watcher_on_event_skips_dir_creation(self):
        with mock.patch("file_watcher.main.logger") as logger:
            with mock.patch("file_watcher.main.Memphis", new=AwaitableNonAsyncMagicMock()) as memphis:
                self.file_watcher = FileWatcher(self.config)
                await self.file_watcher._init()
                producer = AwaitableNonAsyncMagicMock()
                self.file_watcher.producer = producer
                self.file_watcher.connect_to_broker = mock.MagicMock()
                memphis.is_connected = AwaitableNonAsyncMagicMock(return_value=True)

                with tempfile.TemporaryDirectory() as tmpdirname:
                    path = Path(tmpdirname)
                    await self.file_watcher.on_event(path)
                    producer.produce.assert_not_called()
                    logger.info.assert_called_with("Skipping directory creation for: %s", tmpdirname)
                    self.assertEqual(logger.info.call_count, 4)

    @pytest.mark.asyncio
    async def test_file_watcher_start_watching_handles_exceptions_from_watcher(self):
        with mock.patch("file_watcher.main.logger") as logger:
            with mock.patch("file_watcher.main.create_last_run_detector", new=AsyncMock()) as create_last_run_detector:
                with mock.patch("file_watcher.main.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
                    self.file_watcher = FileWatcher(self.config)
                    await self.file_watcher._init()
                    exception = Exception("CRAZY EXCEPTION!")

                    def raise_exception():
                        raise exception
                    create_last_run_detector.return_value.watch_for_new_runs = \
                        AwaitableNonAsyncMagicMock(side_effect=raise_exception)

                    # Should not raise, if raised it does not handle exceptions correctly
                    await self.file_watcher.start_watching()

                    create_last_run_detector.return_value.watch_for_new_runs.assert_called_once_with()
                    logger.info.assert_called_with("File observer fell over watching because of the following "
                                                   "exception:")
                    logger.exception.assert_called_with(exception)

    @pytest.mark.asyncio
    async def test_file_watcher_start_watching_creates_last_run_detector(self):
        with mock.patch("file_watcher.main.create_last_run_detector",
                        new=AsyncMock()) as create_last_run_detector:
            with mock.patch("file_watcher.main.Memphis", new=AwaitableNonAsyncMagicMock()) as _:
                self.file_watcher = FileWatcher(self.config)
                await self.file_watcher._init()

                await self.file_watcher.start_watching()

                create_last_run_detector.return_value.watch_for_new_runs.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_start_creates_config_filewatcher_and_watches(self):
        with mock.patch("file_watcher.main.FileWatcher", new=AwaitableNonAsyncMagicMock()) as file_watcher:
            with mock.patch("file_watcher.main.load_config", new=AwaitableNonAsyncMagicMock()) as load_config_mock:
                await start()

                load_config_mock.assert_called_once_with()
                file_watcher.assert_called_once_with(load_config_mock.return_value)
                file_watcher.return_value._init.assert_called_once_with()
                file_watcher.return_value.start_watching.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_main_starts_asyncio(self):
        with mock.patch("file_watcher.main.start", new=AwaitableNonAsyncMagicMock()) as start_mock:
            with mock.patch("file_watcher.main.asyncio") as asyncio_mock:
                main()

                start_mock.assert_called_once_with()
                asyncio_mock.run.assert_called_once_with(start_mock.return_value)
