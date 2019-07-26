from Consumers import base_cosumer as extend
from Services.telegram_alarm import Telegram
from Services import meta_trader as mt
from threading import Thread


class TaskExecutor(mt.MetaTrader):
    def __init__(self, logger, iqueue, socket):
        self._iqueue = iqueue
        self.logger = logger
        self.socket = socket

    def run(self, **kwargs):
        thread = Thread(target=self.start, kwargs=kwargs)
        return thread.start()

    def start(self):
        socket = self.socket
        logger = self.logger
        while True:
            logger.info('Standby', cname=type(self).__name__)

            data_from_q = self._iqueue.get()

            logger.info('Running', cname=type(self).__name__)

            data_from_q['msg'] = data_from_q['action'] + data_from_q['CODIGO DE NEGOGIACAO DO PAPEL'] +\
                " em :" + str(data_from_q['DATA DO PREGAO'])

            Telegram.send_message(str(data_from_q['msg']))

            # self.metatrader_trade(socket=socket, data=data_from_q)

            self._iqueue.task_done()

    def metatrader_trade(self, socket, data):
        stock_code = data['stock_code']
        self.logger.info('Getting data from Metatrader', cname=type(self).__name__)

        # TODO PEGAR OS DADOS DA OPERACAO A PARTIR DO DATA

        mt_response = self.meta_trader_get_values(socket, 'TRADE|BUY|' + stock_code)

        print(mt_response)
        self.logger.info('MetaTrader response: ' + mt_response, cname=type(self).__name__)
        return mt_response


