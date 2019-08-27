import logging
from importlib import resources


def get_logger(name, level='WARNING'):
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())
    with resources.path('eduscores.logs', '') as path:
        logfile = path / f'{name}.log'
        handler = logging.FileHandler(logfile)
        logger.addHandler(handler)
    return logger
