from Consumers import base_cosumer as extend


class BollingerBandConsumer(extend.BaseConsumer):

    def method(self):
        logger = self.logger
        fill_oqueue = False
        while True:
            logger.info('Standby - Waiting for data on queue', cname=type(self).__name__)
            data = self._iqueue.get()
            logger.info('Running', cname=type(self).__name__)

            if data['BANDA_1_40 SUPERIOR'] == "EMPTY":
                self._iqueue.task_done()
                continue

            if float(data['PRECO FECHAMENTO']) > float(data['BANDA_1_40 SUPERIOR']):
                data['action'] = 'BUY'
                fill_oqueue = True

            elif float(data['PRECO FECHAMENTO']) < float(data['BANDA_1_40 SUPERIOR']):
                data['action'] = 'SELL'
                fill_oqueue = True

            if fill_oqueue:
                self._oqueue.put(data)
                fill_oqueue = False

            self._iqueue.task_done()
