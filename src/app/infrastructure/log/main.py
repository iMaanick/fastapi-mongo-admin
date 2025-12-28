import logging.config
from logging import Logger
from typing import Literal

import structlog
from sqlalchemy import log as sa_log
from structlog.processors import CallsiteParameter, CallsiteParameterAdder

logger = logging.getLogger(__name__)


LoggingLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def _noop_logger_handler(logger: Logger) -> None:
    return None


def configure_logging(level: LoggingLevel = "INFO") -> None:
    # Mute SQLAlchemy default logger handler
    sa_log._add_default_handler = _noop_logger_handler  # noqa: SLF001

    common_processors = (
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.ExtraAdder(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f", utc=True),
        structlog.contextvars.merge_contextvars,
        CallsiteParameterAdder(
            (
                CallsiteParameter.FUNC_NAME,
                CallsiteParameter.LINENO,
            ),
        ),
    )
    structlog_processors = (
        structlog.processors.StackInfoRenderer(),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.UnicodeDecoder(),  # convert bytes to str
        structlog.processors.ExceptionPrettyPrinter(),
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    )
    logging_processors = (
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
    )
    logging_console_processors = (
        *logging_processors,
        structlog.dev.ConsoleRenderer(),
    )

    handler = logging.StreamHandler()
    handler.set_name("default")
    handler.setLevel(level)
    console_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=common_processors,
        processors=logging_console_processors,
    )
    handler.setFormatter(console_formatter)

    handlers: list[logging.Handler] = [handler]

    logging.basicConfig(handlers=handlers, level=level)
    structlog.configure(
        processors=common_processors + structlog_processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    logger.info("Logger successfully setup")
