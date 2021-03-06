import time
from Services import meta_trader as mt
import pandas as pd
from Helper import Constants

HIST_PATH = Constants.WINDOWS_HIST_PATH
BLUE_CHIPS = Constants.WINDOWS_BLUE_CHIPS_PATH

PAIR_PATH = Constants.WINDOWS_PAIR_INFO_PATH
PAIRS = Constants.WINDOWS_PAIRS_PATH


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
        blue_chips = pd.read_csv(BLUE_CHIPS)
        pairs_df = pd.read_csv(PAIRS)
        # For each day
        for each_day in range(245-39):
            # Getting from 39 days after day one of 2018 to fill ma
            day = each_day + 39
            self.logger.info('Day: ' + str(day), cname=type(self).__name__)

            # Long and Short
            for pair in pairs_df['Par']:
                try:
                    formatted_data = pd.read_csv(PAIR_PATH + pair).iloc[day]
                    self.dispatch(formatted_data)
                except Exception as e:
                    self.logger.error(e)

            # # TODO UNC
            # # For each blue_chip
            # for blue_chip in blue_chips['CODIGO DE NEGOCIACAO DO PAPEL']:
            #     try:
            #         formatted_data = pd.read_csv(HIST_PATH + blue_chip + "_2018").iloc[day]
            #         self.dispatch(formatted_data)
            #     except Exception as e:
            #         self.logger.error(e)

    def tracking(self, mt_socket):
        # TODO: FAZER PRIMEIRO O BACK_TESTING
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
            # TODO UNC self.logger.info('Filling queue', cname=type(self).__name__)
            if data is not None:
                queue.put_nowait(data)

    def metatrader_acquisition(self, socket, stock_code):
        self.logger.info('Getting data from Metatrader', cname=type(self).__name__)
        mt_response = self.meta_trader_get_values(socket, 'RATES|' + stock_code)
        print(mt_response)
        self.logger.info('MetaTrader response: ' + mt_response, cname=type(self).__name__)
        return mt_response
