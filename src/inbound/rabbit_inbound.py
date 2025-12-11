import pika
import json
from logger_config import logger
from inbound.base import InboundSource
from db_connection.imo_loader import get_imo
from db_connection.vessel_id import get_id
from db_connection.mapping_db_saver import save_noon_parsing_report
from mapping.mapping_db import build_noon_parsing_payload


class RabbitMQInbound(InboundSource):

    REQUIRED_KEYS = ["sender", "tenant", "subject", "body"]

    def __init__(self, cfg: dict, parser=None, mapper=None):
        self.host = cfg["host"]
        self.port = cfg["port"]
        self.queue = cfg["queue"]
        self.username = cfg["username"]
        self.password = cfg["password"]
        self.vhost = cfg.get("vhost", "/")

        self.parser = parser
        self.mapper = mapper

        self.max_messages = 5
        self.current_count = 0

    def _process_message(self, msg):
        return {k: msg.get(k) for k in self.REQUIRED_KEYS}

    def _callback(self, ch, method, properties, body):
        try:
            raw_msg = json.loads(body.decode("utf-8"))
        except Exception as e:
            logger.error(f"Invalid JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        processed_msg = self._process_message(raw_msg)
        logger.info(f"Received message for tenant {processed_msg['tenant']}")

        mail_body = processed_msg["body"]
        tenant = processed_msg["tenant"]

        try:
            vessel_imo, name = get_imo(mail_body)
            print(vessel_imo)
            print(mail_body)
            parsed = self.parser.parse(mail_body, tenant, vessel_imo)
            mapped = self.mapper.map(parsed, tenant, vessel_imo, name)

            vessel_id = get_id(vessel_imo)
            print("vessel_id", vessel_id)

            payload = build_noon_parsing_payload(mapped, tenant, vessel_id, mail_body, name)

            if payload:
                save_noon_parsing_report(payload)

            logger.info(f"Message processed & saved for tenant {tenant}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        # Count messages
        self.current_count += 1
        if self.current_count >= self.max_messages:
            logger.info(f"Reached limit of {self.max_messages} messages. Stopping consumer.")
            # Use safe thread callback for stopping
            ch.connection.add_callback_threadsafe(ch.stop_consuming)

    def start_worker(self):
        credentials = pika.PlainCredentials(self.username, self.password)

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.vhost,
                credentials=credentials,
                heartbeat=60,                    # prevents disconnects
                blocked_connection_timeout=300   # safe long parsing
            )
        )

        channel = connection.channel()
        channel.queue_declare(queue=self.queue, durable=True)

        # prefetch to control load
        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(
            queue=self.queue,
            on_message_callback=self._callback,
            auto_ack=False
        )

        logger.info(f"RabbitMQ Worker started on queue: {self.queue}")

        try:
            channel.start_consuming()
        except Exception as e:
            logger.error(f"Error while consuming: {e}")
        finally:
            try:
                if channel.is_open:
                    channel.close()
            except: 
                pass
            try:
                if connection.is_open:
                    connection.close()
            except:
                pass

    def fetch(self):
        pass
