import time
from Sevices import meta_trader as mt
import pandas as pd

WINDOWS_PATH = "C:/Users/Rodrigo/Desktop/DOM-BOT/Jupyter/HISTORICO_2018/"
LINUX_PATH = "/home/rodrigo/Desktop/PythonCodes/DOM-BOT/Jupyter/HISTORICO_2018/"
HIST_PATH = WINDOWS_PATH


class Sniffer(mt.MetaTrader):
    """
        Sniffer gets data and fills all the consumers queues
    """
    _queues = []

    def __init__(self, logger):
        self.logger = logger

    def register_queue(self, queue):
        self.logger.info('Appending queue to sniffer', cname=type(self).__name__)
        self._queues.append(queue)
        return queue

    def start(self, mode, socket=''):
        if mode == 'test':
            self.logger.info('Backtesting operation mode', cname=type(self).__name__)
            self.back_testing()
        if mode == 'track':
            self.logger.info('Tracking operation mode', cname=type(self).__name__)
            self.tracking(socket)
        else:
            self.logger.info('Invalid operation mode', cname=type(self).__name__)

    def back_testing(self):
        df = pd.read_csv(HIST_PATH + "ABEV3_2018")

        for i in range(len(df)):
            formatted_data = df.iloc[i]
            self.dispatch(formatted_data)

        # TODO
        #  VOU SEMPRE MANDAR ESSE DATA FRAME -
        #  QUANDO FOR TESTE CONSUMIDOR TEM QUE OLHAR RANGE ABERTURA E FECHAMENTO
        #  QUANDO FOR TRACKING O FECHAMENTO RECEBE O VALOR ATUAL, MAS ANTES ATUALIZA A PARADA

    def tracking(self, mt_socket):
        while True:
            # Todo: loop through other stocks!!!!!!!!!!!!!
            stock_code = 'PETR4'
            # Getting data:
            data = (self.metatrader_acquisition(socket=mt_socket, stock_code=stock_code)).split(',')
            bid = data[0]
            ask = data[1]
            max = data[2]
            min = data[3]
            formatted_data = {
                'code': stock_code,
                'bid': bid,
                'ask': ask,
                'max': max,
                'min': min
            }
            # Dispatching data:
            self.dispatch(formatted_data)
            # Waiting to get next data
            # Sec * Min
            time.sleep(60 * 30)

    def dispatch(self, data):
        # Filling queues
        for queue in self._queues:
            self.logger.info('Filling queue')
            if data is not None:
                queue.put_nowait(data)

    def metatrader_acquisition(self, socket, stock_code):
        self.logger.info('Getting data from Metatrader', cname=type(self).__name__)
        mt_response = self.meta_trader_get_values(socket, 'RATES|' + stock_code)
        print(mt_response)
        self.logger.info('MetaTrader response: ' + mt_response, cname=type(self).__name__)
        return mt_response
