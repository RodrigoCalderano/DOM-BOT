import daiquiri
import logging
from Controllers.sniffer import Sniffer
from Consumers.long_short_consumer import LongShortConsumer
from Consumers.bollinger_band_consumer import BollingerBandConsumer
from Consumers.bollinger_band_v2_consumer import BollingerV2BandConsumer
from Controllers.task_executor import TaskExecutor
from Services import meta_trader as mt
import queue

TEST_MODE = 'test'
TRACK_MODE = 'track'
mode = TEST_MODE


def main():
    # Create logger
    daiquiri.setup(level=logging.INFO)
    logger = daiquiri.getLogger('CM')

    # Get metatrader socket
    # TODO: CHANGE THIS WHEN USING META TRADER
    socket = ''
    # socket = mt.MetaTrader.meta_trader_connector()

    # Entry queues
    long_short_queue = queue.Queue()
    bollinger_band_queue = queue.Queue()
    bollinger_band_v2_queue = queue.Queue()

    # Outer queue
    output_queue = queue.Queue()

    # Create instances
    sniffer = Sniffer(logger=logger)  # Queue filler
    # Outer Handler - Módulo de execução de tarefas (MET)
    t_exec = TaskExecutor(iqueue=output_queue, logger=logger, socket=socket)
    long_short_consumer = LongShortConsumer(iqueue=long_short_queue, oqueue=output_queue, logger=logger)
    bollinger_band_consumer = BollingerBandConsumer(iqueue=bollinger_band_queue, oqueue=output_queue, logger=logger)
    bollinger_band_consumer_v2 = BollingerV2BandConsumer(iqueue=bollinger_band_v2_queue, oqueue=output_queue, logger=logger)

    # Register queues that sniffer will fill
    sniffer.register_queue(long_short_queue)
    sniffer.register_queue(bollinger_band_queue)
    sniffer.register_queue(bollinger_band_v2_queue)

    # TODO UNC: RUN ALL
    # Run the consumers
    long_short_consumer.run()
    # bollinger_band_consumer.run()
    # bollinger_band_consumer_v2.run()

    # Run the Outer Handler
    t_exec.run()

    # After all consumers and the outer handler is running, start filling the queues by running the sniffer
    try:
        status = sniffer.start(mode=mode, socket=socket)
        logger.info("---Terminated---", status=status)
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    main()
