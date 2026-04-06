import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

from config import COMMAND_QOS, LOG_PREFIX
from db import save_command_log

logger = logging.getLogger(__name__)


class CommandService:
    def __init__(self, mqtt_client):
        self.client = mqtt_client

    def send_reset(self, machine_code: str):
        return self._send_command(machine_code, 'reset_counter')

    def send_reboot(self, machine_code: str):
        return self._send_command(machine_code, 'reboot')

    def _send_command(self, machine_code: str, command: str):
        machine_code = (machine_code or '').strip()
        if not machine_code:
            raise ValueError('machine_code is required')

        request_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
        topic = f'factory/machine/{machine_code}/command'

        payload = {
            'command': command,
            'request_id': request_id,
            'ts': datetime.now(timezone.utc).isoformat(),
        }

        payload_json = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
        result = self.client.publish(topic, payload_json, qos=COMMAND_QOS, retain=False)

        ok = result.rc == 0
        if ok:
            save_command_log(machine_code, request_id, command, payload_json)
            logger.info('%s COMMAND sent machine=%s command=%s request_id=%s', LOG_PREFIX, machine_code, command, request_id)
        else:
            logger.error('%s COMMAND failed machine=%s command=%s rc=%s', LOG_PREFIX, machine_code, command, result.rc)

        return {
            'ok': ok,
            'machine_code': machine_code,
            'command': command,
            'request_id': request_id,
            'topic': topic,
        }
