import daiquiri
import logging
from Controllers.sniffer import Sniffer
from Consumers.long_short_consumer import LongShortConsumer
from Consumers.moving_avg_consumer import MovingAverageConsumer
from Controllers.task_executor import TaskExecutor
from Sevices import meta_trader as mt
import queue

mode = 'test'
# mode = 'track'


def main():
    # Create logger
    daiquiri.setup(level=logging.INFO)
    logger = daiquiri.getLogger('CM')

    # Get metatrader socket TODO: TEST THIS!
    socket = ''
    # socket = mt.MetaTrader.meta_trader_connector()

    # Entry queues
    long_short_queue = queue.Queue()
    moving_avg_queue = queue.Queue()

    # Outer queue
    output_queue = queue.Queue()

    # Create instances
    sniffer = Sniffer(logger=logger)  # Queue filler
    # Outer Handler - Módulo de execução de tarefas (MET)
    t_exec = TaskExecutor(iqueue=output_queue, logger=logger, socket=socket)
    long_short_consumer = LongShortConsumer(iqueue=long_short_queue, oqueue=output_queue, logger=logger)
    moving_avg_consumer = MovingAverageConsumer(iqueue=moving_avg_queue, oqueue=output_queue, logger=logger)

    # Register queues that sniffer will fill
    sniffer.register_queue(long_short_queue)
    sniffer.register_queue(moving_avg_queue)

    # Run the consumers
    long_short_consumer.run()
    moving_avg_consumer.run()

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
