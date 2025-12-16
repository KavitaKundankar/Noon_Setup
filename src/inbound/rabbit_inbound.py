from .worker import RabbitWorker
from .callback_handler import CallbackHandler


class RabbitMQInbound:

    def __init__(self, cfg, parser=None, mapper=None , cat = False ):
        self.handler = CallbackHandler(parser, mapper)
        self.cfg = cfg
        self.truthy= cat
        

    def _callback(self, ch, method, properties, body):
        """Proxy callback connecting RabbitMQ → Handler"""
        should_stop = self.handler.process(ch, method, body)

        if should_stop:
            ch.connection.add_callback_threadsafe(ch.stop_consuming)

    def start_worker(self):
        if self.truthy == True:
            return 'Try Again'
        worker = RabbitWorker(self.cfg, self._callback)
        worker.start()
