###########EXTERNAL IMPORTS############

import os
import asyncio
import logging
import json
from fastapi import FastAPI, Request, Header, Form, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from uvicorn import Config, Server
from datetime import datetime, timezone, timedelta
from typing import Dict, Set, Optional, Any
from passlib.hash import pbkdf2_sha256
from dataclasses import dataclass
import jwt
import secrets

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
from util.functions import process_and_save_image
from controller.manager import DeviceManager
from db.db import SQLiteDBClient
from db.timedb import TimeDBClient
from protocol.modbus_rtu.rtu_device import ModbusRTUEnergyMeter
from protocol.opcua.opcua_device import OPCUAEnergyMeter
from controller.conversion import convert_dict_to_energy_meter

#######################################

###############EXCEPTIONS##############

LoggerManager.get_logger(__name__).setLevel(logging.DEBUG)


class InvalidCredentials(Exception):
    """Raised when credentials (username and password) are invalid (not recognized)."""

    pass


@dataclass
class LoginToken:
    """
    Represents an active user session containing the JWT token and session metadata.

    This class is used to manage and track information about an authenticated user's session,
    including their token, associated username, originating IP address, and session persistence settings.

    Attributes:
        token (str): The JWT token issued upon successful login.
        user (str): The username associated with this session.
        ip (str): The IP address from which the user authenticated.
        auto_login (bool): Whether the session should persist without requiring manual login
                           again from the same IP (e.g., "remember me" functionality).
        keep_session_until (Optional[datetime]): If set, defines the timestamp until which the session
                                                 remains valid without re-authentication, even if inactive.
    """

    token: str
    user: str
    ip: str
    auto_login: bool
    keep_session_until: Optional[datetime]


@dataclass
class RequestsSafety:
    """
    Represents security-related tracking data for request attempts on a specific endpoint.

    Attributes:
        endpoint (str): The API endpoint being tracked (e.g., "/login", "/delete_logs").
        count (int): The number of failed attempts made to this endpoint.
        last_attempt_time (Optional[datetime]): The timestamp of the most recent attempt.
        blocked_until (Optional[datetime]): If set, indicates the IP is blocked until this time.
    """

    endpoint: str
    count: int
    last_attempt_time: Optional[datetime]
    blocked_until: Optional[datetime]


class HTTPSafety:
    """
    Provides security mechanisms for HTTP endpoints including user authentication,
    JWT session validation, and per-endpoint request rate limiting by IP address.

    Key Responsibilities:
        - Validate user passwords for minimum complexity requirements.
        - Decode and verify JWT tokens provided in request headers.
        - Track failed request attempts per IP and endpoint to mitigate brute-force attacks.
        - Temporarily block IPs from accessing specific endpoints after repeated failures.
        - Manage active user sessions through the LoginToken structure, allowing optional auto-login logic.

    Attributes:
        USER_CONFIG_PATH (str): Path to the user configuration JSON file containing hashed credentials and the JWT secret.
        MAX_REQUEST_ATTEMPTS (int): Maximum number of failed attempts allowed before an IP is blocked for an endpoint.
        BLOCK_TIME (timedelta): Duration an IP remains blocked after exceeding the failed request limit.
        failed_requests (Dict[str, Dict[str, RequestsSafety]]): Tracks failed attempts per IP and endpoint.
        active_tokens (Dict[str, LoginToken]): Stores active JWT tokens per username, including session metadata like IP,
                                               auto-login status, and keep-alive timestamp.
    """

    USER_CONFIG_PATH = "user_config.json"  # Path to user/password file
    MAX_REQUEST_ATTEMPTS = 5  # Max failed request attempts for sensitive endpoints (login, deletes, ...)
    BLOCK_TIME = timedelta(minutes=15)  # IP Block Time on exceeding max request attempts

    def __init__(self):
        self.failed_requests: Dict[str, Dict[str, RequestsSafety]] = {}
        self.active_tokens: Dict[str, LoginToken] = {}

    def validate_password(self, password: str) -> bool:
        """
        Validates whether a password meets basic security requirements.

        Criteria:
            - Must be at least 5 characters long.
            - Cannot consist of only whitespace.

        Args:
            password (str): The password to validate.

        Returns:
            bool: True if the password is valid, False otherwise.
        """

        return bool(password) and len(password.strip()) >= 5

    def check_authorization_token(self, authorization: Optional[str], request: Optional[Request]) -> str:
        """
        Validates and verifies a JWT token for user authentication.

        This method performs token extraction and verification in the following order:
            1. Attempts to extract the token from the 'Authorization' header using the Bearer scheme.
            2. If the header is missing or invalid, attempts to retrieve the token from request cookies.
            3. Loads the JWT secret key from the local user configuration file.
            4. Decodes and verifies the JWT token using the secret key.
            5. Ensures that the token matches the one stored in memory for the authenticated user.
            6. Confirms that the session's username matches the one encoded in the token.

        If all checks pass, the method returns the authenticated username.

        Args:
            authorization (Optional[str]): The value of the 'Authorization' header (format: "Bearer <token>").
            request (Optional[Request]): The incoming FastAPI request object, used to access cookies if needed.

        Returns:
            str: The username embedded in the validated JWT token.

        Raises:
            ValueError: If no token is found, the token is invalid or expired,
                        or the session does not match the stored user and token.
        """

        token = None

        if authorization and authorization.startswith("Bearer "):
            token = authorization.split(" ")[1]
        else:
            token = request.cookies.get("token")

        if not token:
            raise ValueError("No valid token found")

        with open(HTTPSafety.USER_CONFIG_PATH, "r") as file:
            config: Dict[str, Any] = json.load(file)

        jwt_secret = config.get("jwt_secret")
        payload: Dict[str, Any] = jwt.decode(token, jwt_secret, algorithms=["HS256"])
        username = payload.get("user")

        if self.active_tokens.get(username).token != token or self.active_tokens.get(username).user != username:
            raise ValueError("Token is invalid or session expired")

        return username

    def is_blocked(self, ip: str, endpoint: str) -> bool:
        """
        Checks whether the given IP address is currently blocked for a specific endpoint
        due to too many failed login attempts.

        If the block duration has expired or the last attempt was too long ago, the IP
        entry is removed, and the IP is considered unblocked.

        Args:
            ip (str): The IP address to check.
            endpoint (str): The name of the endpoint being accessed.

        Returns:
            bool: True if the IP is currently blocked for the given endpoint, False otherwise.
        """

        ip_attempts = self.failed_requests.get(ip, {})
        attempt: Optional[RequestsSafety] = ip_attempts.get(endpoint)

        if not attempt:
            return False

        now = datetime.now(timezone.utc)

        if attempt.blocked_until and now < attempt.blocked_until:
            return True

        if attempt.last_attempt_time and now - attempt.last_attempt_time > HTTPSafety.BLOCK_TIME:
            self.clean_failed_requests(ip, endpoint)

        return False

    def clean_failed_requests(self, ip: str, endpoint: str) -> None:
        """
        Removes tracking information related to a specific IP and endpoint combination.

        This method deletes the stored `RequestsSafety` object associated with the provided IP and endpoint.
        If the IP has no other endpoints being tracked afterward, it also removes the IP entry entirely.

        Args:
            ip (str): The IP address whose record should be cleared.
            endpoint (str): The endpoint path for which the tracking data should be removed.
        """

        if ip in self.failed_requests and endpoint in self.failed_requests[ip]:
            del self.failed_requests[ip][endpoint]

        if ip in self.failed_requests and not self.failed_requests[ip]:
            del self.failed_requests[ip]

    def increment_failed_requests(self, ip: str, endpoint: str) -> None:
        """
        Increments the failed request counter for a given IP and endpoint.

        If the previous block has expired, the counter is reset. When the failed count
        reaches the maximum allowed attempts, the IP is temporarily blocked from accessing
        the specified endpoint.

        Args:
            ip (str): The client's IP address.
            endpoint (str): The name of the endpoint being accessed.
        """

        logger = LoggerManager.get_logger(__name__)
        now = datetime.now(timezone.utc)

        ip_record: Dict[str, RequestsSafety] = self.failed_requests.setdefault(ip, {})

        record: RequestsSafety = ip_record.get(endpoint, RequestsSafety(endpoint, 0, None, None))

        # Reset record if block expired
        if record.blocked_until and now >= record.blocked_until:
            record = RequestsSafety(endpoint, 0, None, None)

        record.count += 1
        record.last_attempt_time = now

        if record.count >= HTTPSafety.MAX_REQUEST_ATTEMPTS:
            record.blocked_until = now + HTTPSafety.BLOCK_TIME
            logger.warning(f"IP {ip} blocked from {endpoint} for {HTTPSafety.BLOCK_TIME}.")

        ip_record[endpoint] = record


class HTTPServer:
    """
    Asynchronous HTTP server built with FastAPI for secure and efficient management of energy meter devices.

    This server provides a REST API for managing devices, retrieving real-time and historical data,
    handling secure user authentication, and protecting sensitive operations through token validation
    and request rate limiting.

    Core Responsibilities:
        - Manage a set of energy meters and their nodes via the DeviceManager.
        - Serve historical log data through integration with a TimeDB (InfluxDB) client.
        - Authenticate users using a one-time credential file and JWT-based sessions.
        - Secure endpoints using IP-based blocking after repeated failed attempts.
        - Expose RESTful endpoints for device state, log access, and critical operations (e.g., password change, log deletion).

    Components:
        - `device_manager` (DeviceManager): Interface for device registration, validation, and data retrieval.
        - `timedb` (TimeDBClient): Used to query, filter, and delete time-series logs.
        - `safety` (HTTPSafety): Handles security logic including token validation, password rules, and failed request tracking.
        - `server` (FastAPI): FastAPI application that registers and serves HTTP endpoints.
        - Runs as a background task on instantiation using asyncio's event loop and Uvicorn.

    Endpoints:
        - `POST /login`: Authenticates a user and returns a JWT token.
        - `POST /logout`: Invalidates the session token.
        - `POST /create_login`: Creates the initial credential config with hashed password and signing key.
        - `POST /change_password`: Securely updates the stored password (requires current credentials and token).
        - `GET /get_device_state`: Returns the state metadata of a specified device.
        - `GET /get_all_device_state`: Lists all currently active device states.
        - `GET /get_nodes_state`: Lists node values of a given device, with optional filtering.
        - `GET /get_logs`: Retrieves historical logs from a specific node.
        - `DELETE /delete_logs`: Deletes logs for a specific node on a device.
        - `DELETE /delete_all_logs`: Wipes all logs associated with a device.

    Security Features:
        - JWT-based authentication with in-memory token tracking.
        - IP-based request blocking for brute-force protection.
        - Password policy enforcement (minimum length, non-whitespace).
        - Token/session validation for all sensitive routes.
        - Detailed logging via LoggerManager for auditability and debugging.

    Notes:
        - Only one user is supported (admin-level) to simplify local/edge deployments.
        - The server is intended to be deployed as a local configuration and monitoring endpoint.
        - Authentication is mandatory for any operation that alters or deletes data.
    """

    def __init__(self, host: str, port: int, device_manager: DeviceManager, db: SQLiteDBClient, timedb: TimeDBClient):
        self.host = host
        self.port = port
        self.device_manager = device_manager
        self.db = db
        self.timedb = timedb
        self.safety = HTTPSafety()
        self.server = FastAPI()
        self.server.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
        self.setup_routes()
        self.start()

    def start(self) -> None:
        """
        Starts the HTTP server asynchronously using the current event loop.

        This method creates a background task that runs the FastAPI server using `asyncio.create_task`.
        It should be called once during initialization or startup of the HTTP server component.
        """

        loop = asyncio.get_event_loop()
        self.run_task = loop.create_task(self.run_server())

    async def run_server(self):
        """
        Asynchronously starts the FastAPI HTTP server using Uvicorn.

        This method builds a Uvicorn `Server` with the provided configuration:
            - Binds the server to the specified host and port.
            - Disables live reload.
            - Suppresses default logging output.

        It runs the server within the asyncio event loop.
        """

        config = Config(app=self.server, host=self.host, port=self.port, reload=False, log_level=logging.CRITICAL + 1)
        server = Server(config)
        await server.serve()

    def setup_routes(self):

        @self.server.post("/auto_login")
        async def auto_login(request: Request):
            """
            Attempts to automatically log in a user based on a valid token stored in cookies.

            This endpoint is typically called on initial page load by the frontend to verify if the user's
            session is still valid. If a valid token is found in the request cookies:
                - It is verified using the JWT secret from the config.
                - A new token is generated to refresh the session.
                - The new token is stored in memory and sent back to the client as an HttpOnly cookie.

            Returns:
                JSONResponse:
                    - 200 OK: If the auto-login is successful.
                        {
                            "message": "Auto-login successful",
                            "username": "admin"
                        }
                    - 401 Unauthorized: If the token is missing, invalid, or expired.
                        {
                            "error": "Auto-login failed, please reauthenticate."
                        }
            """

            try:
                username = self.safety.check_authorization_token(None, request)

                # Token is valid, generate a new one to refresh session
                with open(self.safety.USER_CONFIG_PATH, "r") as file:
                    config = json.load(file)

                jwt_secret = config["jwt_secret"]
                new_payload = {"user": username, "iat": datetime.now(timezone.utc).timestamp()}
                new_token = jwt.encode(new_payload, jwt_secret, algorithm="HS256")

                self.safety.active_tokens[username].token = new_token
                auto_login = self.safety.active_tokens[username].auto_login

                response = JSONResponse(content={"message": "Auto-login successful", "username": username})

                response.set_cookie(key="token", value=new_token, httponly=True, secure=True, samesite="None", max_age=3600 if not auto_login else 2592000)

                return response

            except Exception as e:
                return JSONResponse(status_code=401, content={"error": "Auto-login failed, please reauthenticate."})

        @self.server.post("/login")
        async def login(request: Request):
            """
            Authenticates a user and issues a session token via HTTP-only cookie.

            This endpoint handles login requests with brute-force protection per IP and optional auto-login support.

            Expected JSON Payload:
                - username (str): Username to authenticate.
                - password (str): Corresponding plaintext password.
                - auto_login (bool, optional): If true, the session cookie will persist beyond 1 hour.

            Workflow:
                - Verifies that the IP is not temporarily blocked from repeated failed attempts.
                - Loads stored credentials from the local configuration file.
                - Validates the provided username and password using PBKDF2 hashing.
                - If valid, generates a new JWT token and associates it with the IP/session.
                - Stores the token in memory and sets it as an HTTP-only cookie.

            Returns:
                - 200 OK: If login is successful, with a cookie set and a response message.
                    {
                        "message": "Login successful",
                        "username": "<user>"
                    }
                - 401 Unauthorized: Credentials are invalid
                - 429 Too many requests: IP trying to make the request is blocked
                - 500 Internal Error: Server or request error

            Security:
                - Uses PBKDF2 (passlib) for secure password hashing.
                - JWT tokens include the login timestamp (`iat`) and are stored alongside session metadata.
                - Cookie is marked `HttpOnly` and `SameSite=Strict` to prevent access via JavaScript and reduce CSRF/XSS risks.

            Notes:
                - Cookie `secure` is currently set to `False` (use `True` in production with HTTPS).
                - By default, session cookie expires after 1 hour unless `auto_login` is set.
            """

            logger = LoggerManager.get_logger(__name__)
            ip = request.client.host

            try:

                if self.safety.is_blocked(ip, "/login"):
                    unlocked_date = self.safety.failed_requests.get(ip, {}).get("/login").blocked_until.isoformat()
                    return JSONResponse(
                        status_code=429, content={"code": "IP_BLOCKED", "unlocked": unlocked_date, "error": "Too many failed attempts. Try again later."}
                    )

                payload: Dict[str, Any] = await request.json()
                username: str = payload.get("username")
                password: str = payload.get("password")
                auto_login: bool = payload.get("auto_login", False)

                if not username or not password:
                    raise ValueError("Username and password required.")

                if not os.path.exists(self.safety.USER_CONFIG_PATH):
                    raise FileNotFoundError("User configuration file does not exist.")

                with open(self.safety.USER_CONFIG_PATH, "r") as file:
                    config: Dict[str, Any] = json.load(file)

                stored_username = config.get("username")
                stored_hash = config.get("password_hash")
                jwt_secret = config.get("jwt_secret")

                if username != stored_username or not pbkdf2_sha256.verify(password, stored_hash):
                    raise InvalidCredentials("Invalid credentials.")

                token_payload = {"user": username, "iat": datetime.now(timezone.utc).timestamp()}
                token = jwt.encode(token_payload, jwt_secret, algorithm="HS256")

                self.safety.active_tokens[username] = LoginToken(token=token, user=username, ip=ip, auto_login=auto_login, keep_session_until=None)

                self.safety.clean_failed_requests(ip, "/login")

                response = JSONResponse(content={"message": "Login successful", "username": username})

                response.set_cookie(key="token", value=token, httponly=True, secure=True, samesite="None", max_age=3600 if not auto_login else 2592000)

                return response

            except InvalidCredentials as e:

                self.safety.increment_failed_requests(ip, "/login")
                logger.warning(f"Failed login from IP {ip} due to invalid credentials: {e}")
                requests_count = self.safety.failed_requests.get(ip, {}).get("/login").count
                remaining_requests: int = self.safety.MAX_REQUEST_ATTEMPTS - requests_count if requests_count else self.safety.MAX_REQUEST_ATTEMPTS
                if remaining_requests > 0:
                    return JSONResponse(status_code=401, content={"code": "INVALID_CREDENTIALS", "remaining": remaining_requests, "error": str(e)})
                else:
                    unlocked_date = self.safety.failed_requests.get(ip, {}).get("/login").blocked_until.isoformat()
                    return JSONResponse(
                        status_code=429, content={"code": "IP_BLOCKED", "unlocked": unlocked_date, "error": "Too many failed attempts. Try again later."}
                    )

            except Exception as e:

                self.safety.increment_failed_requests(ip, "/login")
                logger.warning(f"Failed login from IP {ip} due to server error: {e}")
                requests_count = self.safety.failed_requests.get(ip, {}).get("/login").count
                remaining_requests: int = self.safety.MAX_REQUEST_ATTEMPTS - requests_count if requests_count else self.safety.MAX_REQUEST_ATTEMPTS
                return JSONResponse(status_code=500, content={"code": "UNKNOWN_ERROR", "remaining": remaining_requests, "error": str(e)})

        @self.server.post("/logout")
        async def logout(request: Request = None, authorization: str = Header(None)):
            """
            Logs out the current user by invalidating their JWT token.

            This endpoint expects the JWT token in the 'Authorization' header
            using the 'Bearer <token>' scheme. It decodes the token to extract
            the username and checks if the token matches the one stored in
            the server's active_tokens dictionary.

            If matched, the token is removed and the user is logged out.

            Returns:
                - 200 OK: Logout successful.
                - 401 Unauthorized: Missing, invalid, or mismatched token.
            """

            logger = LoggerManager.get_logger(__name__)

            try:
                username = self.safety.check_authorization_token(authorization, request)
                del self.safety.active_tokens[username]

                response = JSONResponse(content={"message": "Logout sucessfull"})
                response.delete_cookie("token")
                return response

            except Exception as e:
                logger.warning(f"Logout failed: {e}")
                return JSONResponse(status_code=401, content={"error": str(e)})

        @self.server.post("/create_login")
        async def create_login(request: Request):
            """
            Creates a new user login and stores secure credentials in a local configuration file.

            This endpoint is intended for first-time setup. It will:
            - Reject the request if a login already exists (preventing overwriting).
            - Validate the presence of both 'username' and 'password' fields in the JSON payload.
            - Hash the password securely using PBKDF2.
            - Generate a unique JWT secret key for future token signing.
            - Persist the login credentials and secret in a local JSON file.

            Expected Request JSON:
                {
                    "username": "admin",
                    "password": "your_secure_password"
                }

            Returns:
                - 200 OK: Login created successfully.
                - 400 Bad Request: If the login already exists, fields are missing, or any error occurs.
            """

            logger = LoggerManager.get_logger(__name__)

            try:
                if os.path.exists(self.safety.USER_CONFIG_PATH):
                    return JSONResponse(status_code=400, content={"error": "Login already exists. Cannot overwrite existing configuration."})

                payload: Dict[str, Any] = await request.json()
                username = payload.get("username")
                password = payload.get("password")

                if not username or not password:
                    raise ValueError("Username and password required")

                if not self.safety.validate_password(password):
                    raise ValueError("Password must be at least 5 characters and not just whitespace.")

                hashed_password = pbkdf2_sha256.hash(password)
                jwt_secret = secrets.token_hex(32)

                config = {"username": username, "password_hash": hashed_password, "jwt_secret": jwt_secret}

                with open(self.safety.USER_CONFIG_PATH, "w") as file:
                    json.dump(config, file, indent=4)

                return JSONResponse(content={"message": "Login created successfully."})

            except Exception as e:
                logger.error(f"Failed to create login: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.post("/change_password")
        async def change_password(request: Request, authorization: str = Header(None)):
            """
            Securely changes the password of the configured user.

            Requirements:
                - Valid Bearer token in Authorization header.
                - JSON payload with:
                    - username (str): Current configured username.
                    - old_password (str): Current password.
                    - confirm_old_password (str): Confirmation of current password.
                    - new_password (str): New password to set.

            Behavior:
                - Verifies the JWT token is valid and corresponds to the stored user.
                - Checks that the username in the request matches both the token and the configuration.
                - Validates the current password using pbkdf2 hash.
                - Ensures the old password matches the confirmation.
                - Hashes and stores the new password securely.

            Returns:
                - 200 OK: Password changed successfully.
                - 400 Bad Request: Invalid credentials, mismatched confirmation, or validation error.
            """
            logger = LoggerManager.get_logger(__name__)
            ip = request.client.host

            try:
                if self.safety.is_blocked(ip, "/change_password"):
                    return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

                # Validate token and get username from it
                username_from_token = self.safety.check_authorization_token(authorization, request)

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

                if not self.safety.validate_password(new_password):
                    raise ValueError("Password must be at least 5 characters and not just whitespace.")

                with open(self.safety.USER_CONFIG_PATH, "r") as file:
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

                with open(self.safety.USER_CONFIG_PATH, "w") as file:
                    json.dump(config, file, indent=4)

                self.safety.clean_failed_requests(ip, "/change_password")
                return JSONResponse(content={"message": "Password changed successfully."})

            except Exception as e:
                self.safety.increment_failed_requests(ip, "/change_password")
                logger.warning(f"Failed password change attempt from IP {ip}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.post("/add_device")
        async def add_device(request: Request, authorization: str = Header(None)):
            logger = LoggerManager.get_logger(__name__)
            ip = request.client.host

            try:
                if self.safety.is_blocked(ip, "/add_device"):
                    return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

                # Check if token is valid
                self.safety.check_authorization_token(authorization, request)

                content_type = request.headers.get("content-type", "")

                if content_type.startswith("multipart/form-data"):
                    form = await request.form()

                    device_data_str = form.get("deviceData")
                    device_nodes_str = form.get("deviceNodes")
                    device_image = form.get("deviceImage")

                    if not device_data_str or not device_nodes_str:
                        raise ValueError("Device data and device nodes are required")

                    device_data = json.loads(device_data_str)
                    device_nodes = json.loads(device_nodes_str)

                else:
                    payload = await request.json()
                    device_data = payload.get("deviceData")
                    device_nodes = payload.get("deviceNodes")
                    device_image = None

                device_name = device_data.get("name") if device_data else None

                if not all([device_data, device_nodes]):
                    raise ValueError("All fields are required")

                # Tries to initialize a new energy meter with the given configuration. Throws exception if an error is found in the configuration
                energy_meter: ModbusRTUEnergyMeter | OPCUAEnergyMeter = convert_dict_to_energy_meter(device_data, device_nodes)
                energy_meter_record = energy_meter.get_meter_record()

                device_id = self.db.insert_energy_meter(energy_meter_record)
                if device_id is not None:
                    energy_meter.id = device_id
                    
                    if device_image:
                        process_and_save_image(device_image, device_id, 200)
                    
                    self.device_manager.add_device(energy_meter)

                    self.safety.clean_failed_requests(ip, "/add_device")
                    return JSONResponse(content={"message": "Device added sucessfully."})
                else:
                    raise Exception(f"Could not add device with name {device_name} and id {device_id} in the database.")

            except Exception as e:
                self.safety.increment_failed_requests(ip, "/add_device")
                logger.exception(f"Failed add device attempt from IP {ip}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.post("/edit_device")
        async def edit_device(request: Request, authorization: str = Header(None)):
            logger = LoggerManager.get_logger(__name__)
            ip = request.client.host

            try:
                if self.safety.is_blocked(ip, "/edit_device"):
                    return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

                # Check if token is valid
                self.safety.check_authorization_token(authorization, request)

                content_type = request.headers.get("content-type", "")

                if content_type.startswith("multipart/form-data"):
                    form = await request.form()

                    device_data_str = form.get("deviceData")
                    device_nodes_str = form.get("deviceNodes")
                    device_image = form.get("deviceImage")  # This will be the file

                    if not device_data_str or not device_nodes_str:
                        raise ValueError("Device data and device nodes are required")

                    device_data = json.loads(device_data_str)
                    device_nodes = json.loads(device_nodes_str)

                    if device_image and hasattr(device_image, 'filename'):
                        logger.info(f"Received image file: {device_image.filename}")

                else:
                    payload = await request.json()
                    device_data = payload.get("deviceData")
                    device_nodes = payload.get("deviceNodes")
                    device_image = None

                device_id = device_data.get("id") if device_data else None

                if not all([device_data, device_nodes]):
                    raise ValueError("All fields are required")

                # Tries to initialize a new energy meter with the given configuration. Throws exception if an error is found in the configuration
                energy_meter: ModbusRTUEnergyMeter | OPCUAEnergyMeter = convert_dict_to_energy_meter(device_data, device_nodes)

                device = self.device_manager.get_device(device_id)
                if not device:
                    raise ValueError(f"Device not found with id {device_id}")

                device.stop()
                self.device_manager.delete_device(device)

                if self.db.update_energy_meter(energy_meter.get_meter_record()):
                    
                    # Process and save image if provided
                    if device_image:
                        try:
                            image_path = process_and_save_image(device_image, device_id)
                            logger.info(f"Image updated successfully: {image_path}")
                        except Exception as img_error:
                            logger.warning(f"Failed to process image: {img_error}")
                            # Continue without failing the device update

                    self.device_manager.add_device(energy_meter)

                    self.safety.clean_failed_requests(ip, "/edit_device")
                    return JSONResponse(content={"message": "Device edited sucessfully."})

                else:
                    raise Exception(f"Could not update device with name {device.name if device else 'not found'} and id {device_id} in the database.")

            except Exception as e:
                self.safety.increment_failed_requests(ip, "/edit_device")
                logger.exception(f"Failed edit device attempt from IP {ip}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.delete("/delete_device")
        async def delete_device(request: Request, authorization: str = Header(None)):
            logger = LoggerManager.get_logger(__name__)
            ip = request.client.host

            try:
                if self.safety.is_blocked(ip, "/delete_device"):
                    return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

                # Check if token is valid
                self.safety.check_authorization_token(authorization, request)

                payload = await request.json()

                device_name = payload.get("deviceName")
                device_id = payload.get("deviceID")

                if not all([device_name, device_id]):
                    raise ValueError("All fields are required")

                device = self.device_manager.get_device(device_id)
                if not device:
                    raise ValueError(f"Device not found with id {device_id}")

                if device.name != device_name:
                    raise ValueError(f"Device name does not match request device_name {device_name} for id {device_id}")

                device.stop()
                self.device_manager.delete_device(device)
                if self.db.delete_energy_meter(device.get_meter_record()):

                    self.safety.clean_failed_requests(ip, "/delete_device")
                    return JSONResponse(content={"message": "Device deleted sucessfully."})

                else:
                    raise Exception(f"Could not delete device with name {device.name if device else 'not found'} and id {device_id} from the database.")

            except Exception as e:
                self.safety.increment_failed_requests(ip, "/delete_device")
                logger.warning(f"Failed delete device attempt from IP {ip}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.get("/get_device_state")
        async def get_device_state(request: Request):
            """
            Endpoint to retrieve the current state of a device via GET query parameters.

            Expects one query parameters:
                - id   (int): The unique ID of the device.

            Validates the query parameters and ensures the specified device exists. If valid,
            returns a JSON response with the current device state, including metadata, protocol,
            connection status, and configuration.

            Returns:
                JSONResponse:
                    - 200 OK with the device state if successful.
                    - 400 Bad Request with an error message if validation fails or an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)

            try:
                # Read parameters from the query string, not the JSON body
                id_raw = request.query_params.get("id")

                if not id_raw:
                    raise ValueError("Missing required query parameters: 'id'")

                try:
                    device_id = int(id_raw)
                except ValueError:
                    raise ValueError(f"Invalid device id: {id_raw!r}")

                device = self.device_manager.get_device(device_id)
                if not device:
                    raise KeyError(f"Device with id {device_id} does not exist.")

                return JSONResponse(content=device.get_device_state())

            except Exception as e:
                logger.error(f"Failed to get device state for id={id_raw!r}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.get("/get_all_device_state")
        async def get_all_device_state():
            """
            Endpoint to retrieve the state of all registered devices.

            This endpoint returns a list of all device states currently managed by the DeviceManager.
            Each device state includes:
                - ID
                - Name
                - Protocol
                - Connection status
                - Meter options
                - Meter type

            Returns:
                JSONResponse:
                    - 200 OK with a list of device state dictionaries.
                    - 400 Bad Request with an error message if an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)

            try:
                all_states = [device.get_device_state() for device in self.device_manager.devices]
                return JSONResponse(content=all_states)

            except Exception as e:
                logger.error(f"Failed to retrieve all device states: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.get("/get_nodes_state")
        async def get_nodes_state(request: Request):
            """
            Endpoint to retrieve the state of all nodes in a specific device.

            Expects a JSON payload with:
                - id (int): The unique ID of the device.
                - filter (str, optional): If provided, only return nodes whose names contain this string.

            For each node, it returns:
                - name
                - value
                - unit

            Returns:
                JSONResponse:
                    - 200 OK with a list of node state dictionaries.
                    - 400 Bad Request with an error message if validation fails or an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)
            data: Dict[str, Any] = {}

            try:
                data = await request.json()
                id = data.get("id")
                filter_str = data.get("filter")  # Optional

                if not all([id]):
                    raise ValueError("Missing one or more required fields: 'id'.")

                device = self.device_manager.get_device(id)

                if not device:
                    raise KeyError(f"Device with id {id} does not exist.")

                if filter_str:
                    nodes_state = {node.name: node.get_publish_format() for node in device.nodes if filter_str in node.name}
                else:
                    nodes_state = {node.name: node.get_publish_format() for node in device.nodes}

                return JSONResponse(content=nodes_state)

            except Exception as e:
                logger.error(f"Failed to get node states for device with id {data.get('id', 'unknown')}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.get("/get_nodes_config")
        async def get_nodes_config(request: Request):
            """
            Endpoint to retrieve the configuration of all nodes in a specific device.

            Expects two query parameters:
                - id   (int): The unique ID of the device.
                - filter (str, optional): If provided, only return nodes whose names contain this string.

            Returns:
                JSONResponse:
                    - 200 OK with a list of node configuration dictionaries.
                    - 400 Bad Request with an error message if validation fails or an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)

            try:

                id_raw = request.query_params.get("id")
                filter_str = request.query_params.get("filter")

                if not id_raw:
                    raise ValueError("Missing required query parameters: 'id'")

                device = self.device_manager.get_device(int(id_raw))

                if not device:
                    raise KeyError(f"Device with id {id} does not exist.")

                if filter_str:
                    nodes_config = {}
                    for node in device.nodes:
                        if filter_str in node.name:
                            record = node.get_node_record()
                            record.device_id = int(id_raw)
                            nodes_config[node.name] = record.__dict__
                else:
                    nodes_config = {}
                    for node in device.nodes:
                        record = node.get_node_record()
                        record.device_id = int(id_raw)
                        nodes_config[node.name] = record.__dict__

                return JSONResponse(content=nodes_config)

            except Exception as e:
                logger.error(f"Failed to get device nodes configuration for id={id_raw!r}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.get("/get_logs")
        async def get_logs_from_measurement(request: Request):
            """
            Retrieves historical log data for a specific node (measurement) from a device.

            Requirements:
                - Request Body: JSON object containing:
                    - id (int): Unique ID of the target device.
                    - measurement (str): Node name (measurement) whose logs will be retrieved.
                    - start_time (str, optional): ISO format datetime string for the start of the time range.
                    - end_time (str, optional): ISO format datetime string for the end of the time range.

            Behavior:
                - Validates that the device and measurement exist.
                - Retrieves log data from the TimeDB for the specified measurement.
                - If start_time and end_time are provided, filters results within that time range.
                - Returns the measurement data in JSON format.

            Returns:
                JSONResponse:
                    - 200 OK: If retrieval is successful, returns the log data.
                    - 400 Bad Request: If validation fails or an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)
            data: Dict[str, Any] = {}

            try:
                data = await request.json()
                id = data.get("id")
                measurement = data.get("measurement")
                start_time_str = data.get("start_time")
                end_time_str = data.get("end_time")

                if not all([id, measurement]):
                    raise ValueError("Missing one or more required fields: 'id', 'measurement'.")

                # Optional time range parsing
                start_time = datetime.fromisoformat(start_time_str) if start_time_str else None
                end_time = datetime.fromisoformat(end_time_str) if end_time_str else None

                device = self.device_manager.get_device(id)

                if not device:
                    raise KeyError(f"Device with id {id} does not exist.")

                if not any(measurement == node.name for node in device.nodes):
                    raise KeyError(f"Node with name {measurement} does not exist in device {device.name if device else 'not found'} with id {id}")

                response = self.timedb.get_measurement_data_between(
                    device_name=device.name, device_id=id, measurement=measurement, start_time=start_time, end_time=end_time
                )
                return JSONResponse(content=response)

            except Exception as e:
                logger.error(
                    f"Failed to retrieve logs for device '{device.name if device else 'not found'}' with id {data.get('id', 'unknown')}, "
                    f"measurement '{data.get('measurement', 'unknown')}': {e}"
                )
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.delete("/delete_logs")
        async def delete_logs_from_measurement(request: Request, authorization: str = Header(None)):
            """
            Endpoint to delete log data for a specific node from a device.

            Expects a JSON payload with the following fields:
                - id (int): The unique ID of the device.
                - measurement (str): The name of the node to delete logs for.

            Returns:
                JSONResponse:
                    - 200 OK with success/failure message.
                    - 400 Bad Request with an error message if validation fails or an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)
            ip = request.client.host
            data: Dict[str, Any] = {}

            try:
                if self.safety.is_blocked(ip, "/delete_logs"):
                    return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

                self.safety.check_authorization_token(authorization, request)
                data = await request.json()
                id = data.get("id")
                measurement = data.get("measurement")

                if not all([id, measurement]):
                    raise ValueError("Missing one or more required fields: 'name', 'id', 'measurement'.")

                device = self.device_manager.get_device(id)
                if not device:
                    raise KeyError(f"Device with id {id} does not exist.")

                if not any(measurement == node.name for node in device.nodes):
                    raise KeyError(f"Node with name {measurement} does not exist in device {device.name if device else 'not found'} with id {id}.")

                result = self.timedb.delete_measurement_data(device_name=device.name, device_id=id, measurement=measurement)

                self.safety.clean_failed_requests(ip, "/delete_logs")

                message = (
                    f"Successfully deleted logs for node '{measurement}' from device '{device.name}' (id {id})."
                    if result
                    else f"Failed to delete logs for node '{measurement}' from device '{device.name}' (id {id})."
                )
                return JSONResponse(content={"result": message})

            except Exception as e:
                self.safety.increment_failed_requests(ip, "/delete_logs")
                logger.error(
                    f"Failed to delete logs for device '{device.name if device else 'not found'}' with id {data.get('id', 'unknown')}, "
                    f"measurement '{data.get('measurement', 'unknown')}': {e}"
                )
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.delete("/delete_all_logs")
        async def delete_all_logs(request: Request, authorization: str = Header(None)):
            """
            Deletes all logged measurement data for a specific device.

            Requires:
                - A valid Bearer token passed in the Authorization header.
                - JSON payload with the following fields:
                    - name (str): The name of the device.
                    - id (int): The unique ID of the device.

            Security & Rate Limiting:
                - Verifies the request's JWT token using `check_authorization_token()`.
                - Tracks failed attempts per IP and blocks the endpoint for abusive behavior.
                - Resets the failed attempt counter on success.

            Returns:
                JSONResponse:
                    - 200 OK: If deletion is successful or device existed and was wiped.
                    - 400 Bad Request: If authorization fails, input is invalid, or too many failed attempts were made.
            """

            logger = LoggerManager.get_logger(__name__)
            ip = request.client.host
            data: Dict[str, Any] = {}

            try:
                if self.safety.is_blocked(ip, "/delete_all_logs"):
                    return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

                self.safety.check_authorization_token(authorization, request)
                data = await request.json()
                name = data.get("name")
                id = data.get("id")

                if not all([name, id]):
                    raise ValueError("Missing one or more required fields: 'name', 'id'.")

                result = self.timedb.delete_db(device_name=name, device_id=id)

                self.safety.clean_failed_requests(ip, "/delete_all_logs")

                message = (
                    f"Successfully deleted all logs from device '{name}' (id {id})." if result else f"Failed to delete logs from device '{name}' (id {id})."
                )
                return JSONResponse(content={"result": message})

            except Exception as e:
                self.safety.increment_failed_requests(ip, "/delete_all_logs")
                logger.error(f"Failed to delete all logs for device {data.get('name', 'unknown')} with id {data.get('id', 'unknown')}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})
