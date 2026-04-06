import json
import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

import paho.mqtt.client as mqtt

from config import (
    LOG_PREFIX,
    MQTT_BROKER,
    MQTT_CLIENT_ID,
    MQTT_CONNECT_TIMEOUT_SEC,
    MQTT_KEEPALIVE,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_QOS,
    MQTT_TOPIC_ACK,
    MQTT_TOPIC_STATUS,
    MQTT_TOPIC_TELEMETRY,
    MQTT_USERNAME,
)
from db import enqueue_telemetry, mark_command_ack, update_device_status

logger = logging.getLogger(__name__)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class MqttHandler:
    def __init__(self):
        self.client = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=True)

        if MQTT_USERNAME:
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

    def connect(self) -> None:
        logger.info('%s Connecting MQTT %s:%s', LOG_PREFIX, MQTT_BROKER, MQTT_PORT)
        self.client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
        self.client.socket_timeout = MQTT_CONNECT_TIMEOUT_SEC

    def loop_start(self) -> None:
        self.client.loop_start()

    def loop_stop(self) -> None:
        self.client.loop_stop()

    def disconnect(self) -> None:
        self.client.disconnect()

    def on_connect(self, client, userdata, flags, rc):
        logger.info('%s MQTT connected rc=%s', LOG_PREFIX, rc)
        client.subscribe(MQTT_TOPIC_TELEMETRY, qos=MQTT_QOS)
        client.subscribe(MQTT_TOPIC_ACK, qos=MQTT_QOS)
        client.subscribe(MQTT_TOPIC_STATUS, qos=MQTT_QOS)
        logger.info('%s Subscribed telemetry/ack/status', LOG_PREFIX)

    def on_disconnect(self, client, userdata, rc):
        if rc == 0:
            logger.info('%s MQTT disconnected cleanly', LOG_PREFIX)
        else:
            logger.warning('%s MQTT disconnected rc=%s', LOG_PREFIX, rc)

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        raw = msg.payload.decode('utf-8', errors='ignore')

        try:
            payload = json.loads(raw)
        except Exception:
            logger.warning('%s Invalid JSON topic=%s payload=%s', LOG_PREFIX, topic, raw[:200])
            return

        machine_code = self._normalize_machine_code(payload.get('machine_code'))
        if not machine_code:
            machine_code = self._extract_machine_code_from_topic(topic)

        if not machine_code:
            logger.warning('%s Missing machine_code topic=%s', LOG_PREFIX, topic)
            return

        payload['machine_code'] = machine_code

        try:
            if topic.endswith('/telemetry'):
                self.handle_telemetry(machine_code, payload)
            elif topic.endswith('/status'):
                self.handle_status(machine_code, payload)
            elif topic.endswith('/ack'):
                self.handle_ack(machine_code, payload)
            else:
                logger.debug('%s Ignored topic=%s', LOG_PREFIX, topic)
        except Exception as exc:
            logger.exception('%s Message handling failed topic=%s machine=%s error=%s', LOG_PREFIX, topic, machine_code, exc)

    def handle_telemetry(self, machine_code: str, payload: dict) -> None:
        event_ts = self._normalize_timestamp(payload.get('ts')) or now_iso()
        counter = self._parse_counter(payload.get('counter'))
        run_state = str(payload.get('run_state', 'UNKNOWN')).strip().upper() or 'UNKNOWN'
        ip_addr = self._normalize_optional_text(payload.get('ip'))
        message_id = self._normalize_optional_text(payload.get('message_id')) or self._build_message_id(machine_code, event_ts)

        payload['ts'] = event_ts
        payload['counter'] = counter
        payload['run_state'] = run_state
        payload['message_id'] = message_id

        payload_json = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
        inserted = enqueue_telemetry(machine_code, payload_json, event_ts, message_id)

        update_device_status(
            machine_code=machine_code,
            last_seen_at=event_ts,
            last_counter=counter,
            last_run_state=run_state,
            last_ip=ip_addr,
            last_payload_json=payload_json,
        )

        if inserted:
            logger.info('%s TELEMETRY machine=%s counter=%s run_state=%s message_id=%s', LOG_PREFIX, machine_code, counter, run_state, message_id)
        else:
            logger.info('%s TELEMETRY duplicate ignored machine=%s message_id=%s', LOG_PREFIX, machine_code, message_id)

    def handle_status(self, machine_code: str, payload: dict) -> None:
        event_ts = self._normalize_timestamp(payload.get('ts')) or now_iso()
        counter = self._parse_counter(payload.get('counter'), default=0.0)
        run_state = str(payload.get('run_state', 'UNKNOWN')).strip().upper() or 'UNKNOWN'
        payload['ts'] = event_ts
        payload['counter'] = counter
        payload['run_state'] = run_state
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))

        update_device_status(
            machine_code=machine_code,
            last_seen_at=event_ts,
            last_counter=counter,
            last_run_state=run_state,
            last_ip=self._normalize_optional_text(payload.get('ip')),
            last_payload_json=payload_json,
        )

        logger.info('%s STATUS machine=%s status=%s run_state=%s', LOG_PREFIX, machine_code, payload.get('status', 'unknown'), run_state)

    def handle_ack(self, machine_code: str, payload: dict) -> None:
        request_id = self._normalize_optional_text(payload.get('request_id'))
        ack_payload_json = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))

        if request_id:
            mark_command_ack(machine_code, request_id, ack_payload_json)
            logger.info('%s ACK machine=%s request_id=%s command=%s result=%s', LOG_PREFIX, machine_code, request_id, payload.get('command'), payload.get('result'))
        else:
            logger.warning('%s ACK missing request_id machine=%s payload=%s', LOG_PREFIX, machine_code, ack_payload_json[:200])

    @staticmethod
    def _extract_machine_code_from_topic(topic: str) -> Optional[str]:
        parts = topic.split('/')
        if len(parts) >= 4 and parts[0] == 'factory' and parts[1] == 'machine':
            return MqttHandler._normalize_machine_code(parts[2])
        return None

    @staticmethod
    def _normalize_machine_code(value) -> Optional[str]:
        if value is None:
            return None
        machine_code = str(value).strip()
        return machine_code or None

    @staticmethod
    def _normalize_optional_text(value) -> Optional[str]:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @staticmethod
    def _normalize_timestamp(value) -> Optional[str]:
        if value is None:
            return None
        value = str(value).strip()
        if not value:
            return None
        try:
            value = value.replace('Z', '+00:00')
            return datetime.fromisoformat(value).astimezone(timezone.utc).isoformat()
        except ValueError:
            logger.warning('%s Invalid ts format received: %s', LOG_PREFIX, value)
            return None

    @staticmethod
    def _parse_counter(value, default: float = None) -> float:
        if value is None:
            if default is not None:
                return float(default)
            raise ValueError('counter is required')
        try:
            counter = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f'invalid counter value: {value}') from exc
        if counter < 0:
            raise ValueError('counter must be >= 0')
        return counter

    @staticmethod
    def _build_message_id(machine_code: str, event_ts: str) -> str:
        safe_ts = event_ts.replace(':', '').replace('-', '').replace('+', '').replace('.', '')
        return f'{machine_code}-{safe_ts}-{uuid4().hex[:8]}'
