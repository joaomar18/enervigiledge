###########EXTERNAL IMPORTS############

from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
from analytics.system import SystemMonitor
from web.api.decorator import auth_endpoint, AuthConfigs
from web.dependencies import services

#######################################

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/get_system_realtime_metrics")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_system_metrics(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    system_monitor: SystemMonitor = Depends(services.get_system_monitor),
) -> JSONResponse:
    """Retrieves the system real time performance metrics."""

    return JSONResponse(content=system_monitor.get_realtime_data().get_data())


@router.get("/get_cpu_usage_history")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_cpu_usage_history(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    system_monitor: SystemMonitor = Depends(services.get_system_monitor),
) -> JSONResponse:
    """Retrieves historical CPU usage data."""

    return JSONResponse(content=system_monitor.get_cpu_usage_history())


@router.get("/get_ram_usage_history")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_ram_usage_history(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    system_monitor: SystemMonitor = Depends(services.get_system_monitor),
) -> JSONResponse:
    """Retrieves historical RAM usage data."""

    return JSONResponse(content=system_monitor.get_ram_usage_history())
