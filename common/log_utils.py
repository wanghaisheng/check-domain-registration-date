import logging
from loguru import logger

def setup_logging(logfile=None, level=logging.INFO):
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')
    if logfile:
        logger.add(logfile)

def log_exception(e, msg=None):
    if msg:
        logger.error(f"{msg}: {e}")
    else:
        logger.error(f"Exception: {e}") 