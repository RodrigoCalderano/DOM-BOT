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

            # Strategy data frame
            strategy = pd.read_csv(Constants.WINDOWS_STRATEGIES_PATH + "BollingerBand")
            strategy = strategy.set_index('CODIGO DE NEGOGIACAO DO PAPEL')
            current_state = strategy.loc[stock_code, 'ESTADO']

            # back test data frame
            back_test = pd.read_csv(Constants.WINDOWS_BACK_TEST_PATH + "BollingerBand")
            back_test_id = len(back_test)

            # Looking for entry points
            if current_state == 'PROCURANDO ENTRADA':
                if float(data['PRECO FECHAMENTO']) > float(data['BANDA_1_40 SUPERIOR']):
                    data['action'] = 'SELL'
                    strategy.loc[stock_code, 'ESTADO'] = 'VENDIDO'
                    fill_oqueue = True

                elif float(data['PRECO FECHAMENTO']) < float(data['BANDA_1_40 INFERIOR']):
                    data['action'] = 'BUY'
                    strategy.loc[stock_code, 'ESTADO'] = 'COMPRADO'
                    fill_oqueue = True

            # Looking for close long position
            elif current_state == 'COMPRADO':
                if float(data['PRECO FECHAMENTO']) > float(data['MA_40']):
                    data['action'] = 'SELL'
                    strategy.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    fill_oqueue = True

            # Looking for close short position
            elif current_state == 'VENDIDO':
                if float(data['PRECO FECHAMENTO']) < float(data['MA_40']):
                    data['action'] = 'BUY'
                    strategy.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    fill_oqueue = True

            # Fill outer queue
            if fill_oqueue:
                back_test.loc[back_test_id + 1, 'CODIGO DE NEGOGIACAO DO PAPEL'] = data['CODIGO DE NEGOGIACAO DO PAPEL']
                back_test.loc[back_test_id + 1, 'DATA DO PREGAO'] = int(data['DATA DO PREGAO'])
                back_test.loc[back_test_id + 1, 'PRECO'] = data['PRECO FECHAMENTO']
                back_test.loc[back_test_id + 1, 'OPERACAO'] = data['action']
                self._oqueue.put(data)
                fill_oqueue = False

            # Update data frames and inner queue
            back_test.to_csv(Constants.WINDOWS_BACK_TEST_PATH + "BollingerBand", index=False)
            strategy.to_csv(Constants.WINDOWS_STRATEGIES_PATH + "BollingerBand")
            self._iqueue.task_done()
