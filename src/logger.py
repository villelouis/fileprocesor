import logging

from src import settings

log_format = '%(levelname)s -- %(asctime)s  -- %(processName)s -- %(threadName)s --  %(message)s'

LOGGER_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "CRITICAL": logging.CRITICAL
}


def get_logger(*, app_name):
    logger = logging.getLogger(app_name)
    logger.propagate = False
    level = LOGGER_LEVELS.get(settings.LOG_LEVEL, "INFO")
    logger.setLevel(level)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    formatter = logging.Formatter(log_format, "%Y-%m-%d %H:%M:%S:%ms")
    stream_handler.setFormatter(formatter)
    logging.basicConfig(format=log_format)
    logger.addHandler(stream_handler)
    return logger


log = get_logger(app_name=settings.APP_NAME)

if __name__ == '__main__':
    log.info('test')
