from Consumers import base_cosumer as extend


class MovingAverageConsumer(extend.BaseConsumer):
    """
        Moving average consumer
    """
    def method(self):
        logger = self.logger
        while True:
            logger.info('Standby - Waiting for data on queue', cname=type(self).__name__)

            data = self._iqueue.get()

            # TODO: do stuff with data

            logger.info('Running', cname=type(self).__name__)

            self._oqueue.put({'key': 'Telegram', 'value': {'key': 'BIDI4', 'value': 10.12}})
            self._iqueue.task_done()
