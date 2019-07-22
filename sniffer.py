import time
from Sevices import meta_trader as mt


class Sniffer(mt.MetaTrader):
    """
        Sniffer gets data and fill all the consumers queues
    """
    _queues = []

    def __init__(self, logger):
        self.logger = logger

    def register_queue(self, queue):
        self.logger.info('Appending queue to sniffer', cname=type(self).__name__)
        self._queues.append(queue)
        return queue

    def start(self):
        mt_socket = self.meta_trader_connector()
        while True:
            # Todo: loop through other stocks!!!!!!!!!!!!!
            stock_code = 'PETR4'
            # Getting data:
            data = (self.metatrader_connection(socket=mt_socket, stock_code=stock_code)).split(',')
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
            time.sleep(10)

    def dispatch(self, data):
        # Filling queues
        for queue in self._queues:
            self.logger.info('Filling queue')
            if data is not None:
                queue.put_nowait(data)

    def metatrader_connection(self, socket, stock_code):
        self.logger.info('Getting data from Metatrader', cname=type(self).__name__)
        mt_response = self.meta_trader_get_values(socket, 'RATES|' + stock_code)
        #mt_response = self.meta_trader_get_values(socket, 'TRADE|BUY|' + stock_code)
        print(mt_response)
        self.logger.info('MetaTrader response: ' + mt_response, cname=type(self).__name__)
        return mt_response
