from Consumers import base_cosumer as extend
from Sevices.telegram_alarm import Telegram
from Sevices import meta_trader as mt


class TaskExecutor(extend.BaseConsumer, mt.MetaTrader):
    def __init__(self, logger, iqueue=None):
        super().__init__(logger, iqueue=iqueue)

    def start(self, socket):
        logger = self.logger
        while True:
            logger.info('Standby', cname=type(self).__name__)

            data_from_q = self._iqueue.get()

            logger.info('Running', cname=type(self).__name__)

            if data_from_q['key'] == 'Telegram':
                logger.info('Sending data to telegram: ' + str(data_from_q), cname=type(self).__name__)
                Telegram.send_message(str(data_from_q))

            # TODO AJUSTAR ESSE FINAL AQUI

            if data_from_q['buy'] == 'Telegram':
                self.metatrader_trade(socket=socket, data=data_from_q)
            if data_from_q['sell'] == 'Telegram':
                self.metatrader_trade(socket=socket, data=data_from_q)
            else:
                logger.info('Invalid data on queue: ' + str(data_from_q), cname=type(self).__name__)

            self._iqueue.task_done()

    def metatrader_trade(self, socket, data):
        stock_code = data['stock_code']
        self.logger.info('Getting data from Metatrader', cname=type(self).__name__)

        # TODO PEGAR OS DADOS DA OPERACAO A PARTIR DO DATA

        mt_response = self.meta_trader_get_values(socket, 'TRADE|BUY|' + stock_code)

        print(mt_response)
        self.logger.info('MetaTrader response: ' + mt_response, cname=type(self).__name__)
        return mt_response


