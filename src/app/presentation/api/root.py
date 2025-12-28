from fastapi import APIRouter

from app.presentation.api.example.router import example_router
from app.presentation.api.healthcheck.router import healthcheck_router

root_router = APIRouter()
root_router.include_router(healthcheck_router)
root_router.include_router(example_router)
