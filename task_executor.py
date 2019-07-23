from Consumers import base_cosumer as extend
from Sevices.telegram_alarm import Telegram


class TaskExecutor(extend.BaseConsumer):
    def __init__(self, logger, iqueue=None):
        super().__init__(logger, iqueue=iqueue)

    def method(self):
        logger = self.logger
        while True:
            logger.info('Standby', cname=type(self).__name__)

            data_from_q = self._iqueue.get()

            logger.info('Running', cname=type(self).__name__)

            if data_from_q['key'] == 'Telegram':
                logger.info('Sending data to telegram: ' + str(data_from_q), cname=type(self).__name__)
                Telegram.send_message(str(data_from_q))
            self._iqueue.task_done()


