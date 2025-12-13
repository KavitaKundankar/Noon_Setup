import json
from logger_config import logger
from db_connection.imo_loader import get_imo
from db_connection.vessel_id import get_id
from db_connection.mapping_db_saver import save_noon_parsing_report
from mapping.mapping_db import build_noon_parsing_payload


class CallbackHandler:

    REQUIRED_KEYS = ["sender", "tenant", "subject", "body"]

    def __init__(self, parser, mapper, max_messages=5):
        self.parser = parser
        self.mapper = mapper
        self.max_messages = max_messages
        self.current_count = 0

    def process(self, ch, method, body):
        """Main callback execution."""
        try:
            raw_msg = json.loads(body.decode("utf-8"))
        except Exception as e:
            logger.error(f"Invalid JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        msg = {k: raw_msg.get(k) for k in self.REQUIRED_KEYS}
        tenant = msg["tenant"]

        logger.info(f"Received message for tenant {tenant}")

        mail_body = f"Subject: {msg['subject']}\n Mail_body : {msg['body']}"

        try:
            vessel_imo, vessel_name = get_imo(mail_body)
            parsed = self.parser.parse(mail_body, tenant, vessel_imo)
            mapped = self.mapper.map(parsed, tenant, vessel_imo, vessel_name)

            vessel_id = get_id(vessel_imo)

            payload = build_noon_parsing_payload(
                mapped, tenant, vessel_id, mail_body, vessel_name
            )

            if payload:
                save_noon_parsing_report(payload)

            logger.info(f"Message processed & saved for tenant {tenant} {vessel_name}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        # message limiting
        self.current_count += 1
        return self.current_count >= self.max_messages
