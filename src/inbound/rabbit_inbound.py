import pika
import json
from logger_config import logger
from inbound.base import InboundSource

class RabbitMQInbound(InboundSource):

    REQUIRED_KEYS = ["sender", "tenant", "subject", "body"]

    def __init__(self, cfg: dict, parser=None, mapper=None):
        self.host = cfg["host"]
        self.port = cfg["port"]
        self.queue = cfg["queue"]
        self.username = cfg["username"]
        self.password = cfg["password"]
        self.vhost = cfg.get("vhost", "/")

        # Inject parser + mapper
        self.parser = parser
        self.mapper = mapper

        self.max_messages = 2
        self.current_count = 0

    def _process_message(self, msg):
        return {k: msg.get(k) for k in self.REQUIRED_KEYS}

    def _callback(self, ch, method, properties, body):
        try:
            raw_msg = json.loads(body.decode("utf-8"))
        except Exception as e:
            logger.error(f"Input message is not valid JSON: {e}")
            return
            

        processed_msg = self._process_message(raw_msg)
        # print(raw_msg)
        logger.info(f"Received message for tenant {processed_msg['tenant']}")

        mail_body = processed_msg["body"]
        tenant = processed_msg["tenant"]

        try:
            parsed = self.parser.parse(mail_body, tenant)
            mapped = self.mapper.map(parsed, tenant)

            logger.info(f"Message processed for tenant {tenant}")

            # Only ACK if everything succeeded
            # ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        self.current_count += 1
        if self.current_count >= self.max_messages:
            logger.info(f"Reached limit of {self.max_messages} messages. Stopping consumer.")
            ch.stop_consuming()

    def start_worker(self):
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

        channel.basic_consume(
            queue=self.queue,
            on_message_callback=self._callback,
            auto_ack=False
        )

        logger.info(f" RabbitMQ Worker started on queue: {self.queue}")
        channel.start_consuming()


    def fetch(self):
        # self.start_worker()
        pass

