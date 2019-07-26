from Consumers import base_cosumer as extend


class MovingAverageConsumer(extend.BaseConsumer):
    """
        Moving average consumer
    """
    def method(self):
        logger = self.logger
        fill_oqueue = False
        while True:
            logger.info('Standby - Waiting for data on queue', cname=type(self).__name__)
            data = self._iqueue.get()
            # TODO: do stuff with data

            if data['PRECO FECHAMENTO'] > data['BANDA_1_40 SUPERIOR']:
                data['msg'] = "DE ACORDO COM MOVING AVERAGE VALE A PENA COMPRAR: " + \
                              data['CODIGO DE NEGOGIACAO DO PAPEL'] + " em :" + str(data['DATA DO PREGAO'])
                fill_oqueue = True

            elif data['PRECO FECHAMENTO'] < data['BANDA_1_40 INFERIOR']:
                data['msg'] = "DE ACORDO COM MOVING AVERAGE VALE A PENA Vender: " + \
                              data['CODIGO DE NEGOGIACAO DO PAPEL'] + " em :" + str(data['DATA DO PREGAO'])
                fill_oqueue = True

            logger.info('Running', cname=type(self).__name__)

            if fill_oqueue:
                self._oqueue.put(data)
                fill_oqueue = False

            self._iqueue.task_done()
