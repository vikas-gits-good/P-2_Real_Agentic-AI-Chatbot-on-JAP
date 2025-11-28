import logging
import os
from datetime import datetime
from typing import Literal


def get_logger(log_type: Literal["full", "etl", "flask"] = "etl"):
    log_dirs = {
        "full": os.path.join(os.getcwd(), "logs", "full"),
        "train": os.path.join(os.getcwd(), "logs", "train"),
        "pred": os.path.join(os.getcwd(), "logs", "pred"),
        "etl": os.path.join(os.getcwd(), "logs", "etl"),
        "flask": os.path.join(os.getcwd(), "logs", "flask"),
    }
    log_dir = log_dirs.get(log_type)
    if not log_dir:
        raise ValueError(f"Invalid log_type: {log_type}")

    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{timestamp}_{log_type}.log"
    log_path = os.path.join(log_dir, filename)

    logger = logging.getLogger(f"{log_type}_logger")
    logger.setLevel(logging.INFO)

    # Prevent multiple handlers being added if this logger is called multiple times
    if not logger.handlers:
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.INFO)

        # Custom formatter for line number padding
        class CustomFormatter(logging.Formatter):
            def format(self, record):
                digits = 4
                record.lineno = f"{record.lineno:0{digits}}"
                return super().format(record)

        formatter = CustomFormatter(
            "[%(asctime)s] %(lineno)s %(name)s - %(levelname)s - %(message)s"
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


log_ful = get_logger("full")
log_etl = get_logger("etl")
log_flk = get_logger("flask")
