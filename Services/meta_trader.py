import zmq

SOCKET_LOCAL_HOST = "tcp://localhost:5555"


class MetaTrader:

    @staticmethod
    def meta_trader_connector():
        """
            Establish a connection between MetaTrader 5 and Python through local Sockets with ZeroMQ
            Parameters: null
            Return: socket
        """
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(SOCKET_LOCAL_HOST)
        return socket

    @staticmethod
    def meta_trader_get_values(socket, data):
        """
            Send data to MetaTrader
            Parameters: socket, data: ('RATES|PETR4')
            Parameters: socket, data: ('TRADE|BUY or SELL|PETR4')
            Return: stock data:
            stock data: (bid, ask, max, min)
        """
        try:
            socket.send_string(data)
            msg = socket.recv_string()
            return msg

        except zmq.Again as e:
            print("Something went wrong: " + str(e))
