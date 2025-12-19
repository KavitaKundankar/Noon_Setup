import json
import time
import re
from datetime import datetime
from logger_config import logger

from redis_manager.daily_limit import DailyLimitManager
from db_connection.imo_loader import get_imo
from db_connection.vessel_id import get_id
from db_connection.mapping_db_saver import save_noon_parsing_report
from mapping.mapping_db import build_noon_parsing_payload
from config_loader import load_config


from redis_manager.daily_flag import DailyFlagManager

config = load_config()

max_daily_limit = config.get("daily_limit", 50)


class CallbackHandler:

    REQUIRED_KEYS = ["sender", "tenant", "subject", "body"]

    def __init__(self, parser, mapper):
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

        # Log warning if tenant is missing but continue processing
        if not tenant:
            logger.warning("Tenant not found in message. Will use standard prompt only.")
            tenant = None  # Explicitly set to None for clarity

        # -------------------------
        # Daily limit check (Redis)
        # -------------------------
        if not self.daily_limit.can_parse_today():
            logger.warning("Daily parsing limit reached. Requeuing message.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            DailyFlagManager().setkey(1)
            return

        logger.info(f"Received message for tenant {tenant}")

        mail_body = f"Subject: {msg['subject']}\nMail_body: {msg['body']}"

        # -------------------------
        # Main processing
        # -------------------------
        try:
            result = get_imo(mail_body)
            
            # Handle case when vessel is not found - continue with None values
            if result is None:
                logger.warning(
                    f"Vessel not found in database for tenant {tenant}. Using standard prompt only."
                )
                vessel_imo = None
                vessel_name = None
            else:
                vessel_imo, vessel_name = result

            # Parse and map (will use standard prompt if vessel_imo is None)
            parsed = self.parser.parse(mail_body, tenant, vessel_imo)
            mapped = self.mapper.map(parsed, tenant, vessel_imo, vessel_name)

            # Get vessel_id (will return None if vessel_imo is None)
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

            # time.sleep(120)

            # logger.info(f"Stopping execution at {datetime.now()}")

        # -------------------------
        # Gemini quota handling
        # -------------------------
        except Exception as e:
            error_msg = str(e)

            # Check for quota/rate-limit errors (429 errors)
            if "quota exceeded" in error_msg.lower() or "429" in error_msg or "rate" in error_msg.lower():
                retry_seconds = 60  # Default retry time
                
                # Try to extract the retry delay from error message
                match = re.search(r"retry in ([\d.]+)s", error_msg)
                if match:
                    retry_seconds = int(float(match.group(1)))
                
                logger.warning(
                    f"Gemini API quota exceeded. Sleeping for {retry_seconds + 10}s before requeuing."
                )
                
                # Sleep and then requeue the message
                time.sleep(retry_seconds + 10)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            # For other errors, log and requeue
            logger.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
