###########EXTERNAL IMPORTS############

from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse

#######################################

#############LOCAL IMPORTS#############

from web.dependencies import services
from web.api.decorator import auth_endpoint, AuthConfigs
from web.safety import HTTPSafety

#######################################

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/auto_login")
@auth_endpoint(AuthConfigs.AUTO_LOGIN)
async def auto_login(request: Request, safety: HTTPSafety = Depends(services.get_safety)) -> JSONResponse:
    """Refreshes existing session token for authenticated users."""
    
    username, token = await safety.update_jwt_token(request)
    response = JSONResponse(content={"message": "Auto-login successful", "username": username})
    response.set_cookie(
        key="token", value=token, httponly=True, secure=True, samesite="none", max_age=3600 if not safety.active_tokens[token].auto_login else 2592000
    )
    return response


@router.post("/login")
@auth_endpoint(AuthConfigs.LOGIN)
async def login(request: Request, safety: HTTPSafety = Depends(services.get_safety)) -> JSONResponse:
    """Authenticates user with username/password and creates session token."""

    username, token = await safety.create_jwt_token(request)
    response = JSONResponse(content={"message": "Login successful", "username": username})
    response.set_cookie(
        key="token", value=token, httponly=True, secure=True, samesite="none", max_age=3600 if not safety.active_tokens[token].auto_login else 2592000
    )
    return response


@router.post("/logout")
@auth_endpoint(AuthConfigs.LOGOUT)
async def logout(request: Request, safety: HTTPSafety = Depends(services.get_safety)) -> JSONResponse:
    """Invalidates session token and logs out user."""

    await safety.delete_jwt_token(request)
    response = JSONResponse(content={"message": "Logout sucessfull"})
    response.delete_cookie("token")
    return response


@router.post("/create_login")
@auth_endpoint(AuthConfigs.CREATE_LOGIN)
async def create_login(request: Request, safety: HTTPSafety = Depends(services.get_safety)) -> JSONResponse:
    """Creates initial user account with username and password."""

    await safety.create_user_configuration(request)
    response = JSONResponse(content={"message": "User configuration file created sucessfully"})
    return response


@router.post("change_password")
@auth_endpoint(AuthConfigs.CHANGE_PASSWORD)
async def change_password(request: Request, safety: HTTPSafety = Depends(services.get_safety)) -> JSONResponse:
    """Updates user password after validating current credentials."""

    await safety.change_user_password(request)
    response = JSONResponse(content={"message": "Password changed successfully."})
    return response
