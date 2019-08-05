from Consumers import base_cosumer as extend
from Helper import Constants
import pandas as pd


class BollingerBandConsumer(extend.BaseConsumer):

    def method(self):
        logger = self.logger
        fill_oqueue = False
        while True:
            logger.info('Standby - Waiting for data on queue', cname=type(self).__name__)
            data = self._iqueue.get()
            logger.info('Running', cname=type(self).__name__)

            stock_code = data['CODIGO DE NEGOGIACAO DO PAPEL']

            df = pd.read_csv(Constants.WINDOWS_STRATEGIES_PATH + "BollingerBand")
            df = df.set_index('CODIGO DE NEGOGIACAO DO PAPEL')
            current_state = df.loc[stock_code, 'ESTADO']

            # BACK TEST
            back_test = pd.read_csv(Constants.WINDOWS_BACK_TEST_PATH + "BollingerBand")
            back_test_id = len(back_test)
            print(back_test_id) # TODO COLOCAR O TRADE NO DF

            if current_state == 'PROCURANDO ENTRADA':
                if float(data['PRECO FECHAMENTO']) > float(data['BANDA_1_40 SUPERIOR']):
                    data['action'] = 'BUY'
                    df.loc[stock_code, 'ESTADO'] = 'COMPRADO'
                    fill_oqueue = True

                elif float(data['PRECO FECHAMENTO']) < float(data['BANDA_1_40 SUPERIOR']):
                    data['action'] = 'SELL'
                    df.loc[stock_code, 'ESTADO'] = 'VENDIDO'
                    fill_oqueue = True

            elif current_state == 'COMPRADO':
                if float(data['PRECO FECHAMENTO']) > float(data['MA_40']):
                    data['action'] = 'SELL'
                    df.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    fill_oqueue = True

            elif current_state == 'VENDIDO':
                if float(data['PRECO FECHAMENTO']) < float(data['MA_40']):
                    data['action'] = 'BUY'
                    df.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    fill_oqueue = True

            if fill_oqueue:
                self._oqueue.put(data)
                fill_oqueue = False

            df.to_csv(Constants.WINDOWS_STRATEGIES_PATH + "BollingerBand")
            self._iqueue.task_done()
