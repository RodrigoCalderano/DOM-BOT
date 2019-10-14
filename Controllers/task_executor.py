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

            action = data_from_q['action']
            action_pt = 'comprar ' if action == 'BUY' else 'vender '
            stock_code = data_from_q['CODIGO DE NEGOCIACAO DO PAPEL']
            date = data_from_q['DATA DO PREGAO']
            price = data_from_q['PRECO FECHAMENTO']
            setup = data_from_q['setup']

            Telegram.send_message(setup + '\nHora de ' + action_pt + stock_code + '\nPreco atual: ' + price)

            self.metatrader_trade(socket=socket, data=data_from_q)

            self._iqueue.task_done()

    def metatrader_trade(self, socket, data):
        stock_code = data['CODIGO DE NEGOCIACAO DO PAPEL']
        self.logger.info('Sending order to Metatrader', cname=type(self).__name__)
        mt_response = self.meta_trader_get_values(socket, 'TRADE|' + data['action'] + |' + stock_code)
        self.logger.info('MetaTrader response: ' + mt_response, cname=type(self).__name__)
        Telegram.send_message(mt_response)
        return mt_response


