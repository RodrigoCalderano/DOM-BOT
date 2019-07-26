from Consumers import base_cosumer as extend


class LongShortConsumer(extend.BaseConsumer):
    """
        Long & Short consumer
    """
    def method(self):
        logger = self.logger
        while True:
            # logger.info('Standby - Waiting for data on queue', cname=type(self).__name__)

            data = self._iqueue.get()

            # TODO: do stuff with data

            # logger.info('Running', cname=type(self).__name__)

            #self._oqueue.put({'key': 'Telegram', 'value': {'key': 'PETR4', 'value': 20.56}})
            self._iqueue.task_done()
