import logging
from collections.abc import Callable
from functools import partial

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from starlette.requests import Request

from app.application.change_tracker import ChangeTrackerError
from app.application.exceptions.base import (
    ApplicationError,
    EntityNotFoundError,
)
from app.domain.common.exceptions import AppError

logger = logging.getLogger(__name__)


def setup_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(
        EntityNotFoundError,
        error_handler(404),
    )
    app.add_exception_handler(
        ChangeTrackerError,
        error_handler(500),
    )
    app.add_exception_handler(
        ApplicationError,
        error_handler(500),
    )
    app.add_exception_handler(
        Exception,
        unknown_exception_handler,
    )


def error_handler(status_code: int) -> Callable[..., ORJSONResponse]:
    return partial(app_error_handler, status_code=status_code)


def app_error_handler(
    request: Request,
    err: ApplicationError,
    status_code: int,
) -> ORJSONResponse:
    return handle_error(
        request=request,
        err=err,
        status_code=status_code,
    )


def unknown_exception_handler(
    request: Request,
    err: Exception,
) -> ORJSONResponse:
    logger.exception("Unknown error occurred", exc_info=err)
    text = err.args[0] if len(err.args) > 0 else "Unknown error"
    return ORJSONResponse(
        content={"detail": f"{err.__class__.__name__}: {text}"},
        status_code=500,
    )


def handle_error(
    request: Request,
    err: AppError,
    status_code: int,
) -> ORJSONResponse:
    logger.error("Handle error", exc_info=err, extra={"error": err})
    return ORJSONResponse(
        content={"detail": err.message},
        status_code=status_code,
    )
