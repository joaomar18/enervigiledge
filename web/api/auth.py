###########EXTERNAL IMPORTS############

from typing import Dict, Any
from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse

#######################################

#############LOCAL IMPORTS#############

from web.dependencies import services
from web.api.decorator import auth_endpoint, AuthConfigs
from web.safety import HTTPSafety
import web.exceptions as api_exception
import util.functions.web as web_util
import util.functions.objects as objects

#######################################

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/auto_login")
@auth_endpoint(AuthConfigs.AUTO_LOGIN)
async def auto_login(request: Request, safety: HTTPSafety = Depends(services.get_safety)) -> JSONResponse:
    """Refreshes existing session token for authenticated users."""

    username, token = await safety.update_jwt_token(request)
    response = JSONResponse(content={"message": "Auto-login successful", "username": username})
    response.set_cookie(
        key="token",
        value=token,
        httponly=True,
        secure=True,
        samesite="none",
        max_age=3600 if not safety.active_tokens[token].auto_login else 2592000,
    )
    return response


@router.post("/login")
@auth_endpoint(AuthConfigs.LOGIN)
async def login(request: Request, safety: HTTPSafety = Depends(services.get_safety)) -> JSONResponse:
    """Authenticates user with username/password and creates session token."""

    try:
        payload: Dict[str, Any] = await request.json()  # request payload
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.INVALID_JSON)

    username = payload.get("username")
    if username is None or not isinstance(username, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.AUTH.MISSING_USERNAME)

    password = payload.get("password")
    if password is None or not isinstance(password, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.AUTH.MISSING_PASSWORD)

    print(f"Auto Login: {payload.get('auto_login')}, Type: {type(payload.get('auto_login'))}")
    auto_login: bool = bool(payload.get("auto_login")) if payload.get("auto_login") is not None else False

    username, token = await safety.create_jwt_token(username, password, auto_login, web_util.get_ip_address(request))
    response = JSONResponse(content={"message": "Login successful", "username": username})
    response.set_cookie(
        key="token",
        value=token,
        httponly=True,
        secure=True,
        samesite="none",
        max_age=3600 if not safety.active_tokens[token].auto_login else 2592000,
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

    try:
        payload: Dict[str, Any] = await request.json()  # request payload
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.INVALID_JSON)

    username = payload.get("username")
    if username is None or not isinstance(username, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.AUTH.MISSING_USERNAME)

    password = payload.get("password")
    if password is None or not isinstance(password, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.AUTH.MISSING_PASSWORD)

    await safety.create_user_configuration(username, password)
    response = JSONResponse(content={"message": "User configuration file created sucessfully"})
    return response


@router.post("change_password")
@auth_endpoint(AuthConfigs.CHANGE_PASSWORD)
async def change_password(request: Request, safety: HTTPSafety = Depends(services.get_safety)) -> JSONResponse:
    """Updates user password after validating current credentials."""

    try:
        payload: Dict[str, Any] = await request.json()  # request payload
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.INVALID_JSON)

    username = payload.get("username")
    if username is None or not isinstance(username, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.AUTH.MISSING_USERNAME)
    
    old_password = payload.get("old_password")
    if old_password is None or not isinstance(old_password, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.AUTH.MISSING_OLD_PASSWORD)

    new_password = payload.get("new_password")
    if new_password is None or not isinstance(new_password, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.AUTH.MISSING_NEW_PASSWORD)

    confirm_new_password = payload.get("confirm_new_password")
    if confirm_new_password is None or not isinstance(confirm_new_password, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.AUTH.MISSING_NEW_PASSWORD_CONFIRM)

    await safety.change_user_password(username, old_password, new_password, confirm_new_password)
    response = JSONResponse(content={"message": "Password changed successfully."})
    return response
