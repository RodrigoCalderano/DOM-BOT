from threading import Thread


class BaseConsumer:
    def __init__(self, logger, iqueue=None, oqueue=None):
        self._iqueue = iqueue
        self._oqueue = oqueue
        self.logger = logger

    def run(self, **kwargs):
        thread = Thread(target=self.method, kwargs=kwargs)
        return thread.start()
