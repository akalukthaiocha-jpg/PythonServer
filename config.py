import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'
LOG_DIR = BASE_DIR / 'logs'
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

SQLITE_DB = str(DATA_DIR / 'factory_queue.db')

LOG_PREFIX = os.getenv('LOG_PREFIX', '[FACTORY-IOT]')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_FILE = os.getenv('LOG_FILE', str(LOG_DIR / 'factory-iot.log'))

MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID', 'factory-pi-server')
MQTT_BROKER = os.getenv('MQTT_BROKER', '127.0.0.1')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
MQTT_USERNAME = os.getenv('MQTT_USERNAME', '')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', '')
MQTT_KEEPALIVE = int(os.getenv('MQTT_KEEPALIVE', '60'))
MQTT_CONNECT_TIMEOUT_SEC = int(os.getenv('MQTT_CONNECT_TIMEOUT_SEC', '10'))

MQTT_TOPIC_TELEMETRY = os.getenv('MQTT_TOPIC_TELEMETRY', 'factory/machine/+/telemetry')
MQTT_TOPIC_ACK = os.getenv('MQTT_TOPIC_ACK', 'factory/machine/+/ack')
MQTT_TOPIC_STATUS = os.getenv('MQTT_TOPIC_STATUS', 'factory/machine/+/status')
MQTT_QOS = int(os.getenv('MQTT_QOS', '1'))
COMMAND_QOS = int(os.getenv('COMMAND_QOS', '1'))

LARAVEL_API_URL = os.getenv('LARAVEL_API_URL', 'https://toc-production.8980404.com/api/ingest-production')
LARAVEL_API_TOKEN = os.getenv('LARAVEL_API_TOKEN', 'MM-171-12')
API_TIMEOUT = int(os.getenv('API_TIMEOUT', '2'))
API_BATCH_SIZE = int(os.getenv('API_BATCH_SIZE', '50'))
RETRY_INTERVAL_SEC = int(os.getenv('RETRY_INTERVAL_SEC', '5'))
INITIAL_RETRY_DELAY_SEC = int(os.getenv('INITIAL_RETRY_DELAY_SEC', '5'))
MAX_RETRY_DELAY_SEC = int(os.getenv('MAX_RETRY_DELAY_SEC', '300'))
PROCESSING_STALE_SEC = int(os.getenv('PROCESSING_STALE_SEC', '120'))
MAX_ERROR_TEXT_LEN = int(os.getenv('MAX_ERROR_TEXT_LEN', '1000'))

OFFLINE_TIMEOUT_SEC = int(os.getenv('OFFLINE_TIMEOUT_SEC', '90'))
APP_HEARTBEAT_SEC = int(os.getenv('APP_HEARTBEAT_SEC', '30'))
