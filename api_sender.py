import ast
import json
import logging
import threading
import time
from typing import Any, Dict, List

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import (
    API_BATCH_SIZE,
    API_TIMEOUT,
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
        logger.info('%s ApiSender (BATCH MODE) started', LOG_PREFIX)

        while not self._stop_event.is_set():
            rows = claim_pending_queue(limit=API_BATCH_SIZE)

            if not rows:
                self._stop_event.wait(RETRY_INTERVAL_SEC)
                continue

            self._send_batch(rows)

        self.session.close()
        logger.info('%s ApiSender stopped', LOG_PREFIX)

    def _parse_payload(self, raw_payload: Any) -> Dict[str, Any]:
        """
        รองรับทั้ง:
        1) JSON string ปกติ -> {"machine_code":"ASSET-000114"}
        2) Python dict string เก่า -> {'machine_code':'ASSET-000114'}
        3) dict ที่เป็น object อยู่แล้ว
        """
        if isinstance(raw_payload, dict):
            return raw_payload

        if raw_payload is None:
            raise ValueError('payload_json is None')

        if not isinstance(raw_payload, str):
            raise ValueError(f'payload_json must be str or dict, got {type(raw_payload).__name__}')

        raw_payload = raw_payload.strip()
        if not raw_payload:
            raise ValueError('payload_json is empty')

        # ลอง parse แบบ JSON ก่อน
        try:
            data = json.loads(raw_payload)
            if not isinstance(data, dict):
                raise ValueError('payload_json is not a JSON object')
            return data
        except Exception:
            pass

        # fallback สำหรับข้อมูลเก่าที่เก็บเป็น Python dict string
        try:
            data = ast.literal_eval(raw_payload)
            if not isinstance(data, dict):
                raise ValueError('payload_json is not a dict-like object')
            return data
        except Exception as exc:
            raise ValueError(f'invalid payload_json format: {exc}')

    def _send_batch(self, rows: List[dict]):
        prepared_items = []

        try:
            for row in rows:
                try:
                    payload = self._parse_payload(row['payload_json'])

                    token = str(payload.get('laravel_api_token', '')).strip()
                    if not token:
                        mark_queue_failed(
                            row_id=row['id'],
                            retry_count=row['retry_count'] + 1,
                            error_message='missing laravel_api_token'
                        )
                        continue

                    # ไม่จำเป็นต้องส่ง token ซ้ำไปใน body
                    clean_payload = dict(payload)
                    clean_payload.pop('laravel_api_token', None)

                    prepared_items.append({
                        'row_id': row['id'],
                        'retry_count': row['retry_count'],
                        'token': token,
                        'payload': clean_payload,
                    })

                except Exception as exc:
                    mark_queue_failed(
                        row_id=row['id'],
                        retry_count=row['retry_count'] + 1,
                        error_message=f'parse payload error: {exc}'
                    )

            if not prepared_items:
                return

            # group ตาม token
            grouped = {}
            for item in prepared_items:
                grouped.setdefault(item['token'], []).append(item)

            for token, items in grouped.items():
                payloads = [item['payload'] for item in items]
                row_ids = [item['row_id'] for item in items]

                headers = {
                    "Content-Type": "application/json",
                    "X-INGEST-TOKEN": token
                }
                
                response = self.session.post(
                    LARAVEL_API_URL,
                    headers=headers,
                    json={"data": payloads},
                    timeout=API_TIMEOUT,
                    verify=False  # ใช้ชั่วคราวตอนทดสอบ
                )
                
                if self._is_success_response(response):
                    for row_id in row_ids:
                        mark_queue_sent(
                            row_id=row_id,
                            response_code=response.status_code,
                            response_body="batch ok",
                        )

                    logger.info(
                        "%s BATCH SENT token=%s count=%s",
                        LOG_PREFIX,
                        token,
                        len(row_ids),
                    )
                    continue
                

                error_msg = self._build_error_message(response)

                for item in items:
                    mark_queue_failed(
                        row_id=item['row_id'],
                        retry_count=item['retry_count'] + 1,
                        error_message=error_msg,
                        response_code=response.status_code,
                    )

                logger.warning(
                    "%s BATCH FAIL token=%s count=%s error=%s",
                    LOG_PREFIX,
                    token,
                    len(items),
                    error_msg,
                )

                if response.status_code == 429:
                    time.sleep(10)

        except Exception as exc:
            for item in prepared_items:
                mark_queue_failed(
                    row_id=item['row_id'],
                    retry_count=item['retry_count'] + 1,
                    error_message=str(exc),
                )

            logger.exception(
                "%s BATCH ERROR count=%s error=%s",
                LOG_PREFIX,
                len(prepared_items),
                exc,
            )

    @staticmethod
    def _is_success_response(response: requests.Response) -> bool:
        if not (200 <= response.status_code < 300):
            return False

        content_type = response.headers.get("Content-Type", "")
        if "application/json" not in content_type.lower():
            return False

        try:
            body = response.json()
        except Exception:
            return False

        if isinstance(body, dict) and body.get("ok") is False:
            return False

        return True

    @staticmethod
    def _build_error_message(response: requests.Response) -> str:
        return f"HTTP {response.status_code}: {response.text[:200]}"