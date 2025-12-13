import pika
from logger_config import logger


class RabbitWorker:

    def __init__(self, cfg, callback_func):
        self.host = cfg["host"]
        self.port = cfg["port"]
        self.queue = cfg["queue"]
        self.username = cfg["username"]
        self.password = cfg["password"]
        self.vhost = cfg.get("vhost", "/")

        self.callback_func = callback_func

    def start(self):
        """Handles RabbitMQ connection and message consumption."""
        credentials = pika.PlainCredentials(self.username, self.password)

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.vhost,
                credentials=credentials,
                heartbeat=60,
                blocked_connection_timeout=300
            )
        )

        channel = connection.channel()
        channel.queue_declare(queue=self.queue, durable=True)
        channel.basic_qos(prefetch_count=1)

        logger.info(f"RabbitMQ Worker started on queue: {self.queue}")

        channel.basic_consume(
            queue=self.queue,
            on_message_callback=self.callback_func,
            auto_ack=False
        )

        try:
            channel.start_consuming()
        except Exception as e:
            logger.error(f"Error while consuming: {e}")
        finally:
            if channel.is_open:
                channel.close()
            if connection.is_open:
                connection.close()
