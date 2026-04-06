import json
import logging
import threading
import time
from typing import Any

import requests

from config import (
    API_BATCH_SIZE,
    API_TIMEOUT,
    LARAVEL_API_TOKEN,
    LARAVEL_API_URL,
    LOG_PREFIX,
    RETRY_INTERVAL_SEC,
)
from db import claim_pending_queue, mark_queue_failed, mark_queue_sent

logger = logging.getLogger(__name__)


class ApiSender(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True, name='ApiSender')
        self._stop_event = threading.Event()
        self.session = requests.Session()

    def stop(self):
        self._stop_event.set()

    def run(self):
        logger.info('%s ApiSender started', LOG_PREFIX)

        if not LARAVEL_API_URL or 'your-domain.com' in LARAVEL_API_URL:
            logger.warning('%s LARAVEL_API_URL is not configured for production yet: %s', LOG_PREFIX, LARAVEL_API_URL)
        if not LARAVEL_API_TOKEN or LARAVEL_API_TOKEN == 'YOUR_API_TOKEN':
            logger.warning('%s LARAVEL_API_TOKEN is still default placeholder', LOG_PREFIX)

        while not self._stop_event.is_set():
            rows = claim_pending_queue(limit=API_BATCH_SIZE)
            if not rows:
                self._stop_event.wait(RETRY_INTERVAL_SEC)
                continue

            for row in rows:
                if self._stop_event.is_set():
                    break
                self._send_row(row)

        self.session.close()
        logger.info('%s ApiSender stopped', LOG_PREFIX)

    def _send_row(self, row):
        row_id = row['id']
        retry_count = int(row['retry_count'] or 0)

        try:
            payload = json.loads(row['payload_json'])
            message_id = payload.get('message_id') or row['message_id'] or f'queue-{row_id}'

            headers = {
                'Content-Type': 'application/json',
                'X-INGEST-TOKEN': LARAVEL_API_TOKEN,
                'X-MESSAGE-ID': str(message_id),
            }

            response = self.session.post(
                LARAVEL_API_URL,
                headers=headers,
                json=payload,
                timeout=API_TIMEOUT,
            )

            if self._is_success_response(response):
                mark_queue_sent(
                    row_id=row_id,
                    response_code=response.status_code,
                    response_body=response.text[:1000],
                )
                logger.info('%s SENT queue_id=%s machine=%s message_id=%s', LOG_PREFIX, row_id, row['machine_code'], message_id)
                return

            error_msg = self._build_error_message(response)
            mark_queue_failed(
                row_id=row_id,
                retry_count=retry_count + 1,
                error_message=error_msg,
                response_code=response.status_code,
            )
            logger.warning('%s FAIL queue_id=%s machine=%s error=%s', LOG_PREFIX, row_id, row['machine_code'], error_msg)

        except Exception as exc:
            mark_queue_failed(
                row_id=row_id,
                retry_count=retry_count + 1,
                error_message=str(exc),
            )
            logger.exception('%s ERROR queue_id=%s machine=%s error=%s', LOG_PREFIX, row_id, row['machine_code'], exc)

    @staticmethod
    def _is_success_response(response: requests.Response) -> bool:
        if not (200 <= response.status_code < 300):
            return False

        content_type = response.headers.get('Content-Type', '')
        if 'application/json' not in content_type.lower():
            return True

        try:
            body: Any = response.json()
        except Exception:
            return True

        if isinstance(body, dict) and body.get('ok') is False:
            return False
        return True

    @staticmethod
    def _build_error_message(response: requests.Response) -> str:
        body_preview = response.text[:500].strip().replace('\n', ' ')
        content_type = response.headers.get('Content-Type', '')
        message = f'HTTP {response.status_code}'

        if 'application/json' in content_type.lower():
            try:
                body = response.json()
                if isinstance(body, dict):
                    err = body.get('message') or body.get('error') or body.get('errors') or body
                    message = f'{message}: {err}'
                    return str(message)
            except Exception:
                pass

        if body_preview:
            message = f'{message}: {body_preview}'
        return message
