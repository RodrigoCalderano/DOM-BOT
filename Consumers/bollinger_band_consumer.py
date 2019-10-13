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

            stock_code = data['CODIGO DE NEGOCIACAO DO PAPEL']

            # Strategy data frame
            strategy = pd.read_csv(Constants.WINDOWS_STRATEGIES_PATH + "BollingerBand")
            strategy = strategy.set_index('CODIGO DE NEGOCIACAO DO PAPEL')
            current_state = strategy.loc[stock_code, 'ESTADO']

            # back test data frame
            back_test = pd.read_csv(Constants.WINDOWS_BACK_TEST_PATH + "BollingerBand")
            back_test_id = len(back_test)

            # Looking for entry points
            if current_state == 'PROCURANDO ENTRADA':
                # Trying to buy if above bollinger bands
                if float(data['PRECO FECHAMENTO']) > float(data['BANDA_1_40 SUPERIOR']):
                    data['action'] = 'SELL'
                    strategy.loc[stock_code, 'ESTADO'] = 'VENDIDO'
                    price = data['PRECO FECHAMENTO']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='SELL')
                    fill_oqueue = True

                # Trying to sell if above bollinger bands
                elif float(data['PRECO FECHAMENTO']) < float(data['BANDA_1_40 INFERIOR']):
                    data['action'] = 'BUY'
                    strategy.loc[stock_code, 'ESTADO'] = 'COMPRADO'
                    price = data['PRECO FECHAMENTO']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='BUY')
                    fill_oqueue = True

            # Looking for close long position
            elif current_state == 'COMPRADO':
                if float(data['PRECO FECHAMENTO']) > float(data['MA_40']):
                    data['action'] = 'SELL'
                    strategy.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    price = data['PRECO FECHAMENTO']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='SELL')
                    fill_oqueue = True

            # Looking for close short position
            elif current_state == 'VENDIDO':
                if float(data['PRECO PRECO']) < float(data['MA_40']):
                    data['action'] = 'BUY'
                    strategy.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    price = data['PRECO FECHAMENTO']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='BUY')
                    fill_oqueue = True

            # Fill outer queue
            if fill_oqueue:
                data['setup'] = '[Bollinger Bands]'
                self._oqueue.put(data)
                fill_oqueue = False

            # Update data frame and inner queue
            strategy.to_csv(Constants.WINDOWS_STRATEGIES_PATH + "BollingerBand")
            self._iqueue.task_done()

    def back_test_updater(self, back_test, back_test_id, stock_code, date, price, action):
        back_test.loc[back_test_id + 1, 'CODIGO DE NEGOCIACAO DO PAPEL'] = stock_code
        back_test.loc[back_test_id + 1, 'DATA DO PREGAO'] = date
        back_test.loc[back_test_id + 1, 'PRECO'] = price
        back_test.loc[back_test_id + 1, 'OPERACAO'] = action
        back_test.to_csv(Constants.WINDOWS_BACK_TEST_PATH + "BollingerBand", index=False)

