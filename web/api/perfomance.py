###########EXTERNAL IMPORTS############

from typing import Dict, Any
from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
import util.functions.performance as perf
from web.api.decorator import auth_endpoint, AuthConfigs
from web.dependencies import services

#######################################

router = APIRouter(prefix="/performance", tags=["performance"])

@router.get("/get_system_metrics")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_system_metrics(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
) -> JSONResponse:
    """Retrieves the system performance metrics."""

    output: Dict[str, Any] = {}
    cpu_usage = perf.get_cpu_performance()
    ram_used, ram_total = perf.get_ram_performance()
    output["cpu_usage"] = cpu_usage
    output["ram_used"] = ram_used
    output["ram_total"] = ram_total
    return JSONResponse(content=output)
