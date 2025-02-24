"""Logging functionality methods."""

# from __future__ import annotations

# from loguru import logger


# def init_logging(log_level: str = "INFO") -> None:
#     """Initialize the logger.

#     Once the logger is set up, dumps the log messages held in the queue.

#     :param log_level: The log level to use.
#     :type log_level: str
#     :param message_queue: The message queue.
#     :type message_queue: MessageQueue
#     """
#     logger.remove()
#     logger.add(sink=sys.stdout, level=log_level, format=get_format_log())
#     _early_logs.flush()
#     logger.debug("logger configured")
