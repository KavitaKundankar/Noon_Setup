import json
import time
import re
from logger_config import logger

from redis_manager.daily_limit import DailyLimitManager
from db_connection.imo_loader import get_imo
from db_connection.vessel_id import get_id
from db_connection.mapping_db_saver import save_noon_parsing_report
from mapping.mapping_db import build_noon_parsing_payload
from config_loader import load_config

config = load_config()

max_daily_limit = config.get("daily_limit", 50)


class CallbackHandler:

    REQUIRED_KEYS = ["sender", "tenant", "subject", "body"]

    def __init__(self, parser, mapper, max_daily_limit=20):
        self.parser = parser
        self.mapper = mapper
        self.daily_limit = DailyLimitManager(max_daily_limit)

    def process(self, ch, method, body):
        """RabbitMQ callback execution"""

        # -------------------------
        # Decode message
        # -------------------------
        try:
            raw_msg = json.loads(body.decode("utf-8"))
        except Exception as e:
            logger.error(f"Invalid JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        msg = {k: raw_msg.get(k) for k in self.REQUIRED_KEYS}
        tenant = msg.get("tenant")

        # -------------------------
        # Daily limit check (Redis)
        # -------------------------
        if not self.daily_limit.can_parse_today():
            logger.warning("Daily parsing limit reached. Requeuing message.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        logger.info(f"Received message for tenant {tenant}")

        mail_body = f"Subject: {msg['subject']}\nMail_body: {msg['body']}"

        # -------------------------
        # Main processing
        # -------------------------
        try:
            vessel_imo, vessel_name = get_imo(mail_body)

            parsed = self.parser.parse(mail_body, tenant, vessel_imo)
            mapped = self.mapper.map(parsed, tenant, vessel_imo, vessel_name)

            vessel_id = get_id(vessel_imo)

            payload = build_noon_parsing_payload(
                mapped,
                tenant,
                vessel_id,
                mail_body,
                vessel_name
            )

            if payload:
                save_noon_parsing_report(payload)

            logger.info(
                f"Message processed & saved | tenant={tenant}, vessel={vessel_name}"
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

        # -------------------------
        # Gemini quota handling
        # -------------------------
        except Exception as e:
            error_msg = str(e).lower()

            if "quota exceeded" in error_msg or "rate-limit" in error_msg:
                retry_seconds = 60
                match = re.search(r"retry in (\d+)", error_msg)
                if match:
                    retry_seconds = int(match.group(1))

                logger.warning(
                    f"Gemini quota exceeded. Sleeping {retry_seconds}s before retry."
                )

                time.sleep(retry_seconds + 20)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            logger.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
