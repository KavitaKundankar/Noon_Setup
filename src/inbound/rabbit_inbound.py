import pika
import json
from logger_config import logger
from inbound.base import InboundSource


class RabbitMQInbound(InboundSource):

    REQUIRED_KEYS = ["sender", "tenant", "subject", "body"]

    def __init__(self, cfg: dict):
        self.host = cfg["host"]
        self.port = cfg["port"]
        self.queue = cfg["queue"]
        self.username = cfg["username"]
        self.password = cfg["password"]
        self.vhost = cfg.get("vhost", "/")
        self.message = None

    def _process_message(self, msg):
        return {k: msg.get(k) for k in self.REQUIRED_KEYS}

    def _callback(self, ch, method, properties, body):
        # Parse message
        try:
            msg = json.loads(body.decode("utf-8"))
        except:
            msg = {"raw": body.decode("utf-8")}

        processed = self._process_message(msg)

        logger.info(f"Message received from RabbitMQ for tenant {processed['tenant']}")

        # Save last message only (optional — for returning)
        self.message = processed

        # ACKNOWLEDGE MESSAGE → avoids message loss
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # DO NOT stop consuming (removed)
        # ch.stop_consuming()

    def fetch(self):
        credentials = pika.PlainCredentials(self.username, self.password)

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.vhost,
                credentials=credentials,
            )
        )

        channel = connection.channel()
        channel.queue_declare(queue=self.queue, durable=True)

        # Start continuous consumption
        channel.basic_consume(
            queue=self.queue,
            on_message_callback=self._callback,
            auto_ack=False    # Because we ACK manually
        )

        logger.info(f"RabbitMQ consumer started on queue '{self.queue}'...")

        # Infinite listening loop
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Consumer stopped manually.")
            channel.stop_consuming()

        connection.close()

        return self.message   # returns last message processed
