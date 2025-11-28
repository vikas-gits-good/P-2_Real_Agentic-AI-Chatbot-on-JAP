import sys
import logging

from src.Logging.logger import log_ful


# For raising error
class CustomException(Exception):
    def __init__(self, error: Exception | None = None):
        _, _, exc_tb = sys.exc_info()
        self.lineno = exc_tb.tb_lineno
        self.file_name = exc_tb.tb_frame.f_code.co_filename
        self.log_msg = f"Error: File - {self.file_name} , line - [{self.lineno}], error - [{str(error)}]"

    def __str__(self):
        return self.log_msg


def LogException(
    error: Exception | None = None,
    prefix: str = "Error",
    logger: logging.Logger = log_ful,
):
    _, _, exc_tb = sys.exc_info()
    lineno = exc_tb.tb_lineno if exc_tb else None
    file_name = exc_tb.tb_frame.f_code.co_filename
    log_msg = (
        f"{prefix}: File - {file_name} , line - [{lineno}], error - [{str(error)}]"
    )
    logger.info(log_msg)
