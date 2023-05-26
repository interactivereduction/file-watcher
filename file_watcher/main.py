import asyncio
import logging
import sys
from queue import SimpleQueue

from memphis import Memphis
from memphis.producer import Producer
from watchdog.observers.polling import PollingObserver

from file_watcher.event_handlers import QueueBasedEventHandler

stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


async def setup_producer() -> Producer:
    memphis = Memphis()
    await memphis.connect(host="localhost", username="rundetection", password="password")
    return await memphis.producer(station_name="rundetection", producer_name="producername")


async def main() -> None:
    """
    Main Loop
    :return: None
    """
    producer = await setup_producer()
    queue = SimpleQueue()
    event_handler = QueueBasedEventHandler(queue)
    observer = PollingObserver()
    observer.schedule(event_handler, "./file_watcher")
    observer.start()
    while True:
        if not queue.empty():
            await producer.produce(queue.get())
        await asyncio.sleep(0.4)


if __name__ == '__main__':
    asyncio.run(main())
