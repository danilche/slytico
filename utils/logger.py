import logging
import os
from datetime import date

today = date.today().strftime('%Y%m%d')

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, f"{today}.log")

os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(module_name):
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        console_handler.setFormatter(console_formatter)

        file_handler = logging.FileHandler(LOG_FILE, mode="a") 
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        file_handler.setFormatter(file_formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger
