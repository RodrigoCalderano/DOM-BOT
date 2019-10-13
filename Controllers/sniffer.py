import time
import pandas as pd
import datetime
from datetime import datetime
import pytz

from Services import meta_trader as mt
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

            # # UNC this when not using ls
            # # For each blue_chip
            # for blue_chip in blue_chips['CODIGO DE NEGOCIACAO DO PAPEL']:
            #     try:
            #         formatted_data = pd.read_csv(HIST_PATH + blue_chip + "_2018").iloc[day]
            #         self.dispatch(formatted_data)
            #     except Exception as e:
            #         self.logger.error(e)

    def tracking(self, mt_socket):
        blue_chips = pd.read_csv(BLUE_CHIPS)
        pairs_df = pd.read_csv(PAIRS)

        while True:
            now = datetime.now(pytz.timezone('Brazil/East'))
            current_hour = now.hour
            current_date = now.strftime("%Y%m%d")
            last_data = pd.read_csv(HIST_PATH + 'PETR4' + "_2019")
            last_data = last_data.iloc[len(last_data)-1]['DATA DO PREGAO']
            need_to_add_line = False if current_date == last_data else True

            for blue_chip in blue_chips['CODIGO DE NEGOCIACAO DO PAPEL']:
                try:
                    data = self.fetch_data(stock_code=blue_chip, mt_socket=mt_socket)
                    bid = data['bid']
                    max = data['max']
                    min = data['min']
                    stock = pd.read_csv(HIST_PATH + blue_chip + "_2019")
                    last_40_closes = stock['PRECO FECHAMENTO'].iloc[len(stock) - 41:len(stock) - 1]
                    last_20_closes = stock['PRECO FECHAMENTO'].iloc[len(stock) - 21:len(stock) - 1]

                    print(last_20_closes.std())
                    fresh_data = [current_date, blue_chip,'-',bid,max,min,bid,'-',0,0,0,0,0,0,'-','-','-','-']
                    if need_to_add_line:
                        stock.loc[len(stock)] = fresh_data

                    print(stock.loc[len(stock) - 1])
                except Exception as e:
                    self.logger.error(e)

                break
            break
            print('Tracking Pairs..')
            time.sleep(2)

            for pair in pairs_df['Par']:
                try:
                    pair = pair.split('_')
                    data = self.fetch_data(stock_code=pair[0], mt_socket=mt_socket)
                    print(data)
                    # TODO UPDATE VALUES
                    data = self.fetch_data(stock_code=pair[1], mt_socket=mt_socket)
                    print(data)
                    # TODO UPDATE VALUES
                except Exception as e:
                    self.logger.error(e)
            time.sleep(60*5)

    def fetch_data(self, stock_code, mt_socket):
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
        return formatted_data

    def dispatch(self, data):
        # Filling queues
        for queue in self._queues:
            # TODO UNC self.logger.info('Filling queue', cname=type(self).__name__)
            if data is not None:
                queue.put_nowait(data)

    def metatrader_acquisition(self, socket, stock_code):
        # TODO UNC  self.logger.info('Getting data from Metatrader', cname=type(self).__name__)
        mt_response = self.meta_trader_get_values(socket, 'RATES|' + stock_code)
        # TODO UNC  self.logger.info('MetaTrader response: ' + mt_response, cname=type(self).__name__)
        return mt_response
