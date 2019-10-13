from Consumers import base_cosumer as extend
from Helper import Constants
import pandas as pd


class BollingerV2BandConsumer(extend.BaseConsumer):

    def method(self):
        logger = self.logger
        fill_oqueue = False
        while True:
            logger.info('Standby - Waiting for data on queue', cname=type(self).__name__)
            data = self._iqueue.get()
            logger.info('Running', cname=type(self).__name__)
            stock_code = data['CODIGO DE NEGOCIACAO DO PAPEL']

            # Strategy data frame
            strategy = pd.read_csv(Constants.WINDOWS_STRATEGIES_PATH + "BollingerBandv2")
            strategy = strategy.set_index('CODIGO DE NEGOCIACAO DO PAPEL')
            current_state = strategy.loc[stock_code, 'ESTADO']

            # back test data frame
            back_test = pd.read_csv(Constants.WINDOWS_BACK_TEST_PATH + "BollingerBandv2")
            back_test_id = len(back_test)

            # Check if stock is frozen
            if int(strategy.loc[stock_code, 'FROZEN']) > 0:
                strategy.loc[stock_code, 'FROZEN'] = int(strategy.loc[stock_code, 'FROZEN']) - 1

            # Looking for entry points
            elif current_state == 'PROCURANDO ENTRADA':
                # Trying to buy if above bollinger bands
                if float(data['PRECO MAXIMO']) > float(data['BANDA_1_40 SUPERIOR']):
                    data['action'] = 'SELL'
                    strategy.loc[stock_code, 'ESTADO'] = 'VENDIDO'
                    # Position time
                    strategy.loc[stock_code, 'AUX'] = 1
                    # Check candle gaps
                    if ((float(data['PRECO MINIMO']) < float(data['BANDA_1_40 SUPERIOR'])) and (
                            float(data['PRECO MAXIMO']) > float(data['BANDA_1_40 SUPERIOR']))):
                        price = data['BANDA_1_40 SUPERIOR']
                    else:
                        price = data['PRECO DE ABERTURA']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='SELL')
                    fill_oqueue = True

                # Trying to sell if above bollinger bands
                elif float(data['PRECO MINIMO']) < float(data['BANDA_1_40 INFERIOR']):
                    data['action'] = 'BUY'
                    strategy.loc[stock_code, 'ESTADO'] = 'COMPRADO'
                    # Position time
                    strategy.loc[stock_code, 'AUX'] = 1
                    # Check candle gaps
                    if ((float(data['PRECO MINIMO']) < float(data['BANDA_1_40 INFERIOR'])) and (
                            float(data['PRECO MAXIMO']) > float(data['BANDA_1_40 INFERIOR']))):
                        price = data['BANDA_1_40 INFERIOR']
                    else:
                        price = data['PRECO DE ABERTURA']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='BUY')
                    fill_oqueue = True

            # Looking for close long position
            elif current_state == 'COMPRADO':
                if float(data['PRECO MAXIMO']) > float(data['MA_40']):
                    data['action'] = 'SELL'
                    strategy.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    # Position time
                    strategy.loc[stock_code, 'AUX'] = 1
                    # Check candle gaps
                    if ((float(data['PRECO MINIMO']) < float(data['MA_40'])) and (
                            float(data['PRECO MAXIMO']) > float(data['MA_40']))):
                        price = data['MA_40']
                    else:
                        price = data['PRECO DE ABERTURA']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='SELL')
                    fill_oqueue = True
                    # TIME STOP
                elif int(strategy.loc[stock_code, 'AUX']) > 20:
                    data['action'] = 'SELL'
                    strategy.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    # Position time
                    strategy.loc[stock_code, 'AUX'] = 1
                    price = data['PRECO DE ABERTURA']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='SELL')
                    fill_oqueue = True
                    # PUT IT ON FROZEN STATUS FOR 20 DAYS
                    strategy.loc[stock_code, 'FROZEN'] = 30
                else:
                    strategy.loc[stock_code, 'AUX'] = int(strategy.loc[stock_code, 'AUX']) + 1

            # Looking for close short position
            elif current_state == 'VENDIDO':
                if float(data['PRECO MINIMO']) < float(data['MA_40']):
                    data['action'] = 'BUY'
                    strategy.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    # Position time
                    strategy.loc[stock_code, 'AUX'] = 1
                    # Check candle gaps
                    if ((float(data['PRECO MINIMO']) < float(data['MA_40'])) and (
                            float(data['PRECO MAXIMO']) > float(data['MA_40']))):
                        price = data['MA_40']
                    else:
                        price = data['PRECO DE ABERTURA']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='BUY')
                    fill_oqueue = True
                    # TIME STOP
                elif int(strategy.loc[stock_code, 'AUX']) > 20:
                    data['action'] = 'BUY'
                    strategy.loc[stock_code, 'ESTADO'] = 'PROCURANDO ENTRADA'
                    # Position time
                    strategy.loc[stock_code, 'AUX'] = 1
                    price = data['PRECO DE ABERTURA']
                    self.back_test_updater(back_test=back_test, back_test_id=back_test_id, stock_code=stock_code,
                                           date=data['DATA DO PREGAO'], price=price, action='BUY')
                    fill_oqueue = True
                    # PUT IT ON FROZEN STATUS FOR 20 DAYS
                    strategy.loc[stock_code, 'FROZEN'] = 30
                else:
                    strategy.loc[stock_code, 'AUX'] = int(strategy.loc[stock_code, 'AUX']) + 1

            # Fill outer queue
            if fill_oqueue:
                self._oqueue.put(data)
                fill_oqueue = False

            # Update data frame and inner queue
            strategy.to_csv(Constants.WINDOWS_STRATEGIES_PATH + "BollingerBandv2")
            self._iqueue.task_done()

    def back_test_updater(self, back_test, back_test_id, stock_code, date, price, action):
        back_test.loc[back_test_id + 1, 'CODIGO DE NEGOCIACAO DO PAPEL'] = stock_code
        back_test.loc[back_test_id + 1, 'DATA DO PREGAO'] = date
        back_test.loc[back_test_id + 1, 'PRECO'] = price
        back_test.loc[back_test_id + 1, 'OPERACAO'] = action
        back_test.to_csv(Constants.WINDOWS_BACK_TEST_PATH + "BollingerBandv2", index=False)

