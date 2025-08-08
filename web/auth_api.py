###########EXTERNAL IMPORTS############

import os
import json
from fastapi import APIRouter, Request, Header, Depends
from fastapi.responses import JSONResponse
from typing import Dict, Any
from datetime import datetime, timezone
import jwt
import secrets
from passlib.hash import pbkdf2_sha256

#######################################

#############LOCAL IMPORTS#############

from web.dependencies import http_deps
from web.safety import InvalidCredentials, HTTPSafety, LoginToken
from util.debug import LoggerManager

#######################################

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/auto_login")
async def auto_login(request: Request, safety: HTTPSafety = Depends(http_deps.get_safety)) -> JSONResponse:
    """
    Validates existing session token and generates a new one to refresh the session.

    Args:
        safety: HTTPSafety instance for token validation and management
        request: FastAPI request object containing cookies and client info

    Returns:
        JSONResponse: Success message with refreshed token cookie, or 401 error

    Raises:
        Exception: Returns 401 status if token validation fails
    """

    try:
        username = safety.check_authorization_token(None, request)

        # Token is valid, generate a new one to refresh session
        with open(safety.USER_CONFIG_PATH, "r") as file:
            config = json.load(file)

        jwt_secret = config["jwt_secret"]
        new_payload = {"user": username, "iat": datetime.now(timezone.utc).timestamp()}
        new_token = jwt.encode(new_payload, jwt_secret, algorithm="HS256")

        safety.active_tokens[username].token = new_token
        auto_login = safety.active_tokens[username].auto_login

        response = JSONResponse(content={"message": "Auto-login successful", "username": username})
        response.set_cookie(key="token", value=new_token, httponly=True, secure=True, samesite="None", max_age=3600 if not auto_login else 2592000)

        return response

    except Exception as e:
        return JSONResponse(status_code=401, content={"error": "Auto-login failed, please reauthenticate."})


@router.post("/login")
async def login(request: Request, safety: HTTPSafety = Depends(http_deps.get_safety)) -> JSONResponse:
    """
    Authenticates user with username/password and creates a new session token.

    Args:
        safety: HTTPSafety instance for security validation and token management
        request: FastAPI request containing JSON payload with credentials

    Returns:
        JSONResponse: Success with token cookie (200), invalid credentials (401),
        IP blocked (429), or server error (500)

    Raises:
        InvalidCredentials: When username/password combination is invalid
        Exception: For server errors, missing files, or malformed requests
    """

    logger = LoggerManager.get_logger(__name__)
    ip = request.client.host

    try:

        if safety.is_blocked(ip, request.url.path):
            unlocked_date = safety.failed_requests.get(ip, {}).get(request.url.path).blocked_until.isoformat()
            return JSONResponse(
                status_code=429, content={"code": "IP_BLOCKED", "unlocked": unlocked_date, "error": "Too many failed attempts. Try again later."}
            )

        payload: Dict[str, Any] = await request.json()
        username: str = payload.get("username")
        password: str = payload.get("password")
        auto_login: bool = payload.get("auto_login", False)

        if not username or not password:
            raise ValueError("Username and password required.")

        if not os.path.exists(safety.USER_CONFIG_PATH):
            raise FileNotFoundError("User configuration file does not exist.")

        with open(safety.USER_CONFIG_PATH, "r") as file:
            config: Dict[str, Any] = json.load(file)

        stored_username = config.get("username")
        stored_hash = config.get("password_hash")
        jwt_secret = config.get("jwt_secret")

        if username != stored_username or not pbkdf2_sha256.verify(password, stored_hash):
            raise InvalidCredentials("Invalid credentials.")

        token_payload = {"user": username, "iat": datetime.now(timezone.utc).timestamp()}
        token = jwt.encode(token_payload, jwt_secret, algorithm="HS256")

        safety.active_tokens[username] = LoginToken(token=token, user=username, ip=ip, auto_login=auto_login, keep_session_until=None)

        safety.clean_failed_requests(ip, request.url.path)

        response = JSONResponse(content={"message": "Login successful", "username": username})

        response.set_cookie(key="token", value=token, httponly=True, secure=True, samesite="None", max_age=3600 if not auto_login else 2592000)

        return response

    except InvalidCredentials as e:

        safety.increment_failed_requests(ip, request.url.path)
        logger.warning(f"Failed login from IP {ip} due to invalid credentials: {e}")
        requests_count = safety.failed_requests.get(ip, {}).get(request.url.path).count
        remaining_requests: int = safety.MAX_REQUEST_ATTEMPTS - requests_count if requests_count else safety.MAX_REQUEST_ATTEMPTS
        if remaining_requests > 0:
            return JSONResponse(status_code=401, content={"code": "INVALID_CREDENTIALS", "remaining": remaining_requests, "error": str(e)})
        else:
            unlocked_date = safety.failed_requests.get(ip, {}).get(request.url.path).blocked_until.isoformat()
            return JSONResponse(
                status_code=429, content={"code": "IP_BLOCKED", "unlocked": unlocked_date, "error": "Too many failed attempts. Try again later."}
            )

    except Exception as e:

        safety.increment_failed_requests(ip, request.url.path)
        logger.warning(f"Failed login from IP {ip} due to server error: {e}")
        requests_count = safety.failed_requests.get(ip, {}).get(request.url.path).count
        remaining_requests: int = safety.MAX_REQUEST_ATTEMPTS - requests_count if requests_count else safety.MAX_REQUEST_ATTEMPTS
        return JSONResponse(status_code=500, content={"code": "UNKNOWN_ERROR", "remaining": remaining_requests, "error": str(e)})


@router.post("/logout")
async def logout(request: Request, authorization: str = Header(None), safety: HTTPSafety = Depends(http_deps.get_safety)) -> JSONResponse:
    """
    Invalidates the current session token and logs out the user.

    Args:
        safety: HTTPSafety instance for token validation and management
        request: FastAPI request object for token extraction from cookies
        authorization: Optional authorization header with Bearer token

    Returns:
        JSONResponse: Success message with deleted token cookie, or 401 error

    Raises:
        Exception: Returns 401 status if token validation fails or user not found
    """

    logger = LoggerManager.get_logger(__name__)

    try:

        username = safety.check_authorization_token(authorization, request)
        del safety.active_tokens[username]

        response = JSONResponse(content={"message": "Logout sucessfull"})
        response.delete_cookie("token")
        return response

    except Exception as e:
        logger.warning(f"Logout failed: {e}")
        return JSONResponse(status_code=401, content={"error": str(e)})


@router.post("/create_login")
async def create_login(request: Request, safety: HTTPSafety = Depends(http_deps.get_safety)) -> JSONResponse:
    """
    Creates initial user account with username and password if none exists.

    Args:
        safety: HTTPSafety instance for password validation and config management
        request: FastAPI request containing JSON payload with username/password

    Returns:
        JSONResponse: Success message (200) or error if account exists/invalid data (400)

    Raises:
        ValueError: When username/password missing or password doesn't meet requirements
        Exception: For file I/O errors or other server issues
    """

    logger = LoggerManager.get_logger(__name__)

    try:
        if os.path.exists(safety.USER_CONFIG_PATH):
            return JSONResponse(status_code=400, content={"error": "Login already exists. Cannot overwrite existing configuration."})

        payload: Dict[str, Any] = await request.json()
        username = payload.get("username")
        password = payload.get("password")

        if not username or not password:
            raise ValueError("Username and password required")

        if not safety.validate_password(password):
            raise ValueError("Password must be at least 5 characters and not just whitespace.")

        hashed_password = pbkdf2_sha256.hash(password)
        jwt_secret = secrets.token_hex(32)

        config = {"username": username, "password_hash": hashed_password, "jwt_secret": jwt_secret}

        with open(safety.USER_CONFIG_PATH, "w") as file:
            json.dump(config, file, indent=4)

        return JSONResponse(content={"message": "Login created successfully."})

    except Exception as e:
        logger.error(f"Failed to create login: {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})


@router.post("change_password")
async def change_password(request: Request, authorization: str = Header(None), safety: HTTPSafety = Depends(http_deps.get_safety)) -> JSONResponse:
    """
    Updates user password after validating current credentials and token.

    Args:
        safety: HTTPSafety instance for token/password validation and rate limiting
        request: FastAPI request containing JSON with old/new password fields
        authorization: Authorization header with Bearer token for authentication

    Returns:
        JSONResponse: Success message (200) or error for validation failures (400)

    Raises:
        ValueError: When fields missing, passwords don't match, or validation fails
        Exception: For token validation errors or file I/O issues
    """

    logger = LoggerManager.get_logger(__name__)
    ip = request.client.host

    try:
        if safety.is_blocked(ip, request.url.path):
            return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

        # Validate token and get username from it
        username_from_token = safety.check_authorization_token(authorization, request)

        payload: Dict[str, str] = await request.json()
        username = payload.get("username")
        old_password = payload.get("old_password")
        confirm_old_password = payload.get("confirm_old_password")
        new_password = payload.get("new_password")

        if not all([username, old_password, confirm_old_password, new_password]):
            raise ValueError("All fields are required")

        if username_from_token != username:
            raise ValueError("Token does not match the provided username")

        if old_password != confirm_old_password:
            raise ValueError("Old password confirmation does not match")

        if not safety.validate_password(new_password):
            raise ValueError("Password must be at least 5 characters and not just whitespace.")

        with open(safety.USER_CONFIG_PATH, "r") as file:
            config: Dict[str, Any] = json.load(file)

        stored_username = config.get("username")
        stored_hash = config.get("password_hash")

        if username != stored_username:
            raise ValueError("Invalid username")

        if not pbkdf2_sha256.verify(old_password, stored_hash):
            raise ValueError("Old password is incorrect")

        # Generate new hash and update config
        new_hash = pbkdf2_sha256.hash(new_password)
        config["password_hash"] = new_hash

        with open(safety.USER_CONFIG_PATH, "w") as file:
            json.dump(config, file, indent=4)

        safety.clean_failed_requests(ip, request.url.path)
        return JSONResponse(content={"message": "Password changed successfully."})

    except Exception as e:
        safety.increment_failed_requests(ip, request.url.path)
        logger.warning(f"Failed password change attempt from IP {ip}: {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})
