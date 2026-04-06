import logging
import signal
import sys
import threading
import time
from logging.handlers import RotatingFileHandler

from api_sender import ApiSender
from command_service import CommandService
from config import APP_HEARTBEAT_SEC, LOG_FILE, LOG_LEVEL, LOG_PREFIX
from db import get_queue_summary, init_db, reset_stuck_processing
from mqtt_handler import MqttHandler

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    root = logging.getLogger()
    if root.handlers:
        return

    level = getattr(logging, LOG_LEVEL, logging.INFO)
    root.setLevel(level)

    formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)s %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5, encoding='utf-8')
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


class AppRuntime:
    def __init__(self):
        self.stop_event = threading.Event()
        self.mqtt_handler = None
        self.api_sender = None
        self.command_service = None

    def start(self) -> None:
        configure_logging()
        logger.info('%s Service starting', LOG_PREFIX)

        init_db()
        recovered = reset_stuck_processing()
        if recovered:
            logger.warning('%s Recovered stale PROCESSING rows: %s', LOG_PREFIX, recovered)

        self.mqtt_handler = MqttHandler()
        self.mqtt_handler.connect()
        self.mqtt_handler.loop_start()

        self.api_sender = ApiSender()
        self.api_sender.start()

        self.command_service = CommandService(self.mqtt_handler.client)
        logger.info('%s Service started successfully', LOG_PREFIX)

    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        logger.info('%s Service stopping', LOG_PREFIX)

        if self.api_sender:
            self.api_sender.stop()
            self.api_sender.join(timeout=10)

        if self.mqtt_handler:
            try:
                self.mqtt_handler.loop_stop()
                self.mqtt_handler.disconnect()
            except Exception:
                logger.exception('%s Error while disconnecting MQTT', LOG_PREFIX)

        logger.info('%s Service stopped', LOG_PREFIX)

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            summary = get_queue_summary()
            logger.info('%s Heartbeat queue=%s', LOG_PREFIX, summary)
            self.stop_event.wait(APP_HEARTBEAT_SEC)


def main() -> None:
    runtime = AppRuntime()

    def _shutdown_handler(signum, frame):
        logger.info('%s Received signal=%s', LOG_PREFIX, signum)
        runtime.stop()

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    try:
        runtime.start()
        runtime.run_forever()
    except KeyboardInterrupt:
        runtime.stop()
    except Exception:
        logger.exception('%s Fatal error in main loop', LOG_PREFIX)
        runtime.stop()
        raise


if __name__ == '__main__':
    main()
