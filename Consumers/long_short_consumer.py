from Consumers import base_cosumer as extend
from Helper import Constants
import pandas as pd


class LongShortConsumer(extend.BaseConsumer):
    """
        Long & Short consumer
    """

    def method(self):
        logger = self.logger
        fill_oqueue = False
        while True:
            logger.info('Standby - Waiting for data on queue', cname=type(self).__name__)
            data = self._iqueue.get()
            logger.info('Running', cname=type(self).__name__)

            stock_code = data['Par']

            # Strategy data frame
            strategy = pd.read_csv(Constants.WINDOWS_STRATEGIES_PATH + "LongShort")
            strategy = strategy.set_index('CODIGO DE NEGOCIACAO DO PAPEL')
            current_state = strategy.loc[stock_code, 'ESTADO']

            # back test data frame (Starts with 0 - Empty)
            back_test = pd.read_csv(Constants.WINDOWS_BACK_TEST_PATH + "LongShort")
            back_test_id = len(back_test)

            # Looking for entry points
            if current_state == 'PROCURANDO ENTRADA':
                # Trying to buy if above bollinger bands
                if float(data['PRECO FECHAMENTO']) > float(data['BANDA_1_40 SUPERIOR']):
                    data['action'] = 'SELL'
                    strategy.loc[stock_code, 'ESTADO'] = 'VENDIDO'
                    strategy.loc[stock_code, 'AUX'] = 1
                    # Dont need to check for candle gaps

                    price = data['BANDA_1_40 SUPERIOR']

                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='SELL')
                    fill_oqueue = True

                # Trying to sell if above bollinger bands
                elif float(data['PRECO FECHAMENTO']) < float(data['BANDA_1_40 INFERIOR']):
                    data['action'] = 'BUY'
                    strategy.loc[stock_code, 'ESTADO'] = 'COMPRADO'

                    price = data['BANDA_1_40 INFERIOR']
                    strategy.loc[stock_code, 'AUX'] = 1
                    # Dont need to check for candle gaps

                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='BUY')
                    fill_oqueue = True

            # Looking for close long position
            elif current_state == 'COMPRADO':
                if float(data['PRECO FECHAMENTO']) > float(data['MA_40']):
                    data['action'] = 'SELL'
                    strategy.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    price = data['MA_40']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='SELL')
                    fill_oqueue = True

            # Looking for close short position
            elif current_state == 'VENDIDO':
                if float(data['PRECO FECHAMENTO']) < float(data['MA_40']):
                    data['action'] = 'BUY'
                    strategy.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    price = data['MA_40']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='BUY')
                    fill_oqueue = True

            # if str(strategy.loc[stock_code, 'AUX']) == '1' or str(strategy.loc[stock_code, 'AUX']) == '2':
            #     # Looking for extend long position
            #     ads
            #     # Looking for extend short position


            # Fill outer queue
            if fill_oqueue:
                self._oqueue.put(data)
                fill_oqueue = False

            # Update data frame and inner queue
            strategy.to_csv(Constants.WINDOWS_STRATEGIES_PATH + "LongShort")
            self._iqueue.task_done()

    def back_test_updater(self, back_test, back_test_id, stock_code, date, price, action):
        back_test.loc[back_test_id + 1, 'CODIGO DE NEGOCIACAO DO PAPEL'] = stock_code
        back_test.loc[back_test_id + 1, 'DATA DO PREGAO'] = date
        back_test.loc[back_test_id + 1, 'PRECO'] = price
        back_test.loc[back_test_id + 1, 'OPERACAO'] = action
        back_test.to_csv(Constants.WINDOWS_BACK_TEST_PATH + "LongShort", index=False)
