###########EXTERNAL IMPORTS############

import os
import asyncio
import logging
import json
from fastapi import FastAPI, Request, Header
from fastapi.responses import JSONResponse
from uvicorn import Config, Server
from datetime import datetime, timezone, timedelta
from typing import Dict, Set, Optional, Any
from passlib.hash import pbkdf2_sha256
import jwt
import secrets

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
from controller.manager import DeviceManager
from db.timedb import TimeDBClient

#######################################

USER_CONFIG_PATH = "user_config.json"  # Path to user/password file


class HTTPServer:

    MAX_REQUEST_ATTEMPTS = 5  # Max failed request attempts for sensitive endpoints (login, deletes, ...)
    BLOCK_TIME = timedelta(minutes=15)  # IP Block Time on exceeding max request attempts

    """
    Asynchronous HTTP server built with FastAPI to manage energy meter devices,
    authentication, node data access, and historical logging operations.

    Core Responsibilities:
        - Provide secure login system using JWT tokens.
        - Allow creation of a one-time user credential file.
        - Interface with the DeviceManager to query and validate registered devices and nodes.
        - Interact with the time-series database (InfluxDB) to serve and manage historical logs.

    Components:
        - `device_manager` (DeviceManager): Access and manage device and node instances.
        - `timedb` (TimeDBClient): Interface to query and delete logs from the InfluxDB.
        - `active_tokens` (Dict[str, str]): In-memory store of active session tokens and it's users (JWT-based).
        - `FastAPI` server: Handles async HTTP requests.
        - Automatically starts via background task when instantiated.

    Endpoints:
        - `POST /login`: Authenticates a user and returns a JWT token.
        - `POST /logout`: Invalidates the user's session by removing their token from memory.
        - `POST /create_login`: Creates a one-time user config file with hashed credentials.
        - `GET /get_device_state`: Returns state metadata of a specific device.
        - `GET /get_all_device_state`: Lists the state of all active devices.
        - `GET /get_nodes_state`: Lists all nodes of a specific device, with optional filtering.
        - `GET /get_logs`: Retrieves historical logs from a specific node and time range.
        - `POST /delete_logs`: Deletes logs from a specific device/node combination.
        - `POST /delete_all_logs`: Deletes the full log history for a device.

    Notes:
        - Authentication is required for protected routes.
        - JWT tokens are stored in memory and are removed on logout or server restart.
        - Endpoint behavior includes error handling and detailed logging via LoggerManager.
    """

    def __init__(self, host: str, port: int, device_manager: DeviceManager, timedb: TimeDBClient):
        self.host = host
        self.port = port
        self.device_manager = device_manager
        self.timedb = timedb
        self.server = FastAPI()
        self.active_tokens: Dict[str, str] = {}
        self.failed_requests: Dict[str, Dict[str, Dict[str, Any]]] = {}  # Structure: { ip: { endpoint: { count, last_attempt_time, blocked_until } } }
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

    def check_authorization_token(self, authorization: str) -> str:
        """
        Validates the provided Bearer token from the Authorization header.

        This method performs the following checks:
            - Ensures the header exists and follows the "Bearer <token>" format.
            - Loads the JWT secret from the local configuration file.
            - Decodes and verifies the token using the stored secret.
            - Confirms that the token matches the active session stored in memory.

        If all checks pass, it returns the username associated with the token.

        Args:
            authorization (str): The value of the Authorization header (expected format: "Bearer <token>").

        Returns:
            str: The username embedded in the token if validation is successful.

        Raises:
            ValueError: If the token is missing, malformed, invalid, or no longer part of an active session.
        """

        if not authorization or not authorization.startswith("Bearer "):
            raise ValueError("Authorization header missing or malformed")

        token = authorization.split(" ")[1]

        with open(USER_CONFIG_PATH, "r") as file:
            config: Dict[str, Any] = json.load(file)

        jwt_secret = config.get("jwt_secret")

        payload: Dict[str, Any] = jwt.decode(token, jwt_secret, algorithms=["HS256"])
        username = payload.get("user")

        if self.active_tokens.get(username) != token:
            raise ValueError("Token is invalid or session expired")

        return username

    def is_blocked(self, ip: str, endpoint: str) -> bool:
        """
        Checks whether the given IP address is currently blocked due to too many failed login attempts.

        If the block duration has expired or the last attempt was too long ago, the IP is unblocked and cleared.

        Args:
            ip (str): The IP address to check.
            endpoint (str): The name of the endpoint trying to be used

        Returns:
            bool: True if the IP is currently blocked for the endpoint given, False otherwise.
        """

        record = self.failed_requests.get(ip, {}).get(endpoint)

        if not record:
            return False

        now = datetime.now(timezone.utc)
        blocked_until = record.get("blocked_until")
        last_attempt = record.get("last_attempt_time")

        # Still blocked
        if blocked_until and now < blocked_until:
            return True

        # Clean if old
        if last_attempt and now - last_attempt > HTTPServer.BLOCK_TIME:
            self.clean_failed_requests(ip, endpoint)

        return False

    def clean_failed_requests(self, ip: str, endpoint: str) -> None:
        """
        Clears the failed login tracking record for the given IP address, removing any count or block state.

        Args:
            ip (str): The IP address to clear.
            endpoint (str): The name of the endpoint trying to be used
        """

        if ip in self.failed_requests and endpoint in self.failed_requests[ip]:
            del self.failed_requests[ip][endpoint]

        if ip in self.failed_requests and not self.failed_requests[ip]:
            del self.failed_requests[ip]

    def increment_failed_requests(self, ip: str, endpoint: str) -> None:
        """
        Increments the failed login attempt counter for the given IP.

        If the time since the last failed attempt exceeds the block duration,
        the attempt count is reset. When the maximum number of attempts is reached,
        the IP is blocked for a defined period.

        Args:
            ip (str): The IP address to track.
            endpoint (str): The name of the endpoint trying to be used
        """

        logger = LoggerManager.get_logger(__name__)

        now = datetime.now(timezone.utc)

        ip_record = self.failed_requests.setdefault(ip, {})
        attempt = ip_record.get(endpoint, {"count": 0, "last_attempt_time": now, "blocked_until": None})

        # Reset if block expired
        if attempt.get("blocked_until") and now >= attempt["blocked_until"]:
            attempt = {"count": 0, "last_attempt_time": now, "blocked_until": None}

        attempt["count"] += 1
        attempt["last_attempt_time"] = now

        if attempt["count"] >= HTTPServer.MAX_REQUEST_ATTEMPTS:
            attempt["blocked_until"] = now + HTTPServer.BLOCK_TIME
            logger.warning(f"IP {ip} blocked from {endpoint} for {HTTPServer.BLOCK_TIME}.")

        ip_record[endpoint] = attempt

    def setup_routes(self):

        @self.server.post("/login")
        async def login(request: Request):
            """
            Handles user login authentication via POST request.

            Implements IP-based login attempt tracking and blocking.

            Expects:
                - username (str)
                - password (str)

            Returns:
                - 200 OK: If credentials are correct
                - 400 Bad Request: If missing fields, invalid credentials, or IP is temporarily blocked
            """

            logger = LoggerManager.get_logger(__name__)
            ip = request.client.host

            try:

                if self.is_blocked(ip, "/login"):
                    return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

                payload: Dict[str, Any] = await request.json()
                username = payload.get("username")
                password = payload.get("password")

                if not username or not password:
                    raise ValueError("Username and password required.")

                if not os.path.exists(USER_CONFIG_PATH):
                    raise FileNotFoundError("User configuration file does not exist.")

                with open(USER_CONFIG_PATH, "r") as file:
                    config: Dict[str, Any] = json.load(file)

                stored_username = config.get("username")
                stored_hash = config.get("password_hash")
                jwt_secret = config.get("jwt_secret")

                if username != stored_username or not pbkdf2_sha256.verify(password, stored_hash):
                    raise ValueError("Invalid credentials.")

                token_payload = {"user": username, "iat": datetime.now(timezone.utc).timestamp()}
                token = jwt.encode(token_payload, jwt_secret, algorithm="HS256")

                self.active_tokens[username] = token

                self.clean_failed_requests(ip, "/login")

                return {"token": token}

            except Exception as e:

                self.increment_failed_requests(ip, "/login")
                logger.warning(f"Failed login from IP {ip}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.post("/logout")
        async def logout(authorization: str = Header(None)):
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
                username = self.check_authorization_token(authorization)
                del self.active_tokens[username]

                return {"message": "Logout successful"}

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
                if os.path.exists(USER_CONFIG_PATH):
                    return JSONResponse(status_code=400, content={"error": "Login already exists. Cannot overwrite existing configuration."})

                payload: Dict[str, Any] = await request.json()
                username = payload.get("username")
                password = payload.get("password")

                if not username or not password:
                    raise ValueError("Username and password required")

                if not self.validate_password(password):
                    raise ValueError("Password must be at least 5 characters and not just whitespace.")

                hashed_password = pbkdf2_sha256.hash(password)
                jwt_secret = secrets.token_hex(32)

                config = {"username": username, "password_hash": hashed_password, "jwt_secret": jwt_secret}

                with open(USER_CONFIG_PATH, "w") as file:
                    json.dump(config, file, indent=4)

                return {"message": "Login created successfully."}

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
                if self.is_blocked(ip, "/change_password"):
                    return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

                # Validate token and get username from it
                username_from_token = self.check_authorization_token(authorization)

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

                if not self.validate_password(new_password):
                    raise ValueError("Password must be at least 5 characters and not just whitespace.")

                with open(USER_CONFIG_PATH, "r") as file:
                    config = json.load(file)

                stored_username = config.get("username")
                stored_hash = config.get("password_hash")

                if username != stored_username:
                    raise ValueError("Invalid username")

                if not pbkdf2_sha256.verify(old_password, stored_hash):
                    raise ValueError("Old password is incorrect")

                # Generate new hash and update config
                new_hash = pbkdf2_sha256.hash(new_password)
                config["password_hash"] = new_hash

                with open(USER_CONFIG_PATH, "w") as file:
                    json.dump(config, file, indent=4)

                self.clean_failed_requests(ip, "/change_password")
                return {"message": "Password changed successfully."}

            except Exception as e:
                self.increment_failed_requests(ip, "/change_password")
                logger.warning(f"Failed password change attempt from IP {ip}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.get("/get_device_state")
        async def get_device_state(request: Request):
            """
            Endpoint to retrieve the current state of a device.

            Expects a JSON payload with:
                - name (str): The name of the device.
                - id (int): The unique ID of the device.

            Validates the request payload and ensures the specified device exists. If valid,
            returns a JSON response with the current device state, including metadata, protocol,
            connection status, and configuration.

            Returns:
                JSONResponse:
                    - 200 OK with the device state if successful.
                    - 400 Bad Request with an error message if validation fails or an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)
            data: Dict[str, Any] = {}

            try:
                data = await request.json()
                name = data.get("name")
                id = data.get("id")

                if not all([name, id]):
                    raise ValueError("Missing one or more required fields: 'name', 'id'.")

                device = self.device_manager.get_device(name, id)

                if not device:
                    raise KeyError(f"Device with name {name} and id {id} does not exist.")

                return JSONResponse(content=device.get_device_state())

            except Exception as e:
                logger.error(f"Failed to get device '{data.get('name', 'unknown')}' with id {data.get('id', 'unknown')} state: {e}")
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
                - name (str): The name of the device.
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
                name = data.get("name")
                id = data.get("id")
                filter_str = data.get("filter")  # Optional

                if not all([name, id]):
                    raise ValueError("Missing one or more required fields: 'name', 'id'.")

                device = self.device_manager.get_device(name, id)

                if not device:
                    raise KeyError(f"Device with name {name} and id {id} does not exist.")

                if filter_str:
                    nodes_state = {node.name: node.get_publish_format() for node in device.nodes if filter_str in node.name}
                else:
                    nodes_state = {node.name: node.get_publish_format() for node in device.nodes}

                return JSONResponse(content=nodes_state)

            except Exception as e:
                logger.error(f"Failed to get node states for device '{data.get('name', 'unknown')}' with id {data.get('id', 'unknown')}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.get("/get_logs")
        async def get_logs_from_measurement(request: Request):
            """
            Deletes log data for a specific node (measurement) from a device.

            Requirements:
                - Authorization: Must provide a valid JWT token via the Authorization header ("Bearer <token>").
                - Request Body: JSON object containing:
                    - name (str): Name of the target device.
                    - id (int): Unique ID of the target device.
                    - measurement (str): Node name (measurement) whose logs will be deleted.

            Behavior:
                - Validates that the device and measurement exist.
                - Ensures the provided token is valid and corresponds to the active session.
                - Tracks failed login attempts per IP for this endpoint, blocking further attempts if abuse is detected.
                - Automatically resets failed attempt count after a successful request.

            Returns:
                JSONResponse:
                    - 200 OK: If deletion is successful.
                    - 400 Bad Request: If validation, authorization, or deletion fails.
            """

            logger = LoggerManager.get_logger(__name__)
            data: Dict[str, Any] = {}

            try:
                data = await request.json()
                name = data.get("name")
                id = data.get("id")
                measurement = data.get("measurement")
                start_time_str = data.get("start_time")
                end_time_str = data.get("end_time")

                if not all([name, id, measurement]):
                    raise ValueError("Missing one or more required fields: 'name', 'id', 'measurement'.")

                # Optional time range parsing
                start_time = datetime.fromisoformat(start_time_str) if start_time_str else None
                end_time = datetime.fromisoformat(end_time_str) if end_time_str else None

                device = self.device_manager.get_device(name, id)

                if not device:
                    raise KeyError(f"Device with name {name} and id {id} does not exist.")

                if not any(measurement == node.name for node in device.nodes):
                    raise KeyError(f"Node with name {measurement} does not exist in device {name} with id {id}")

                response = self.timedb.get_measurement_data_between(
                    device_name=name, device_id=id, measurement=measurement, start_time=start_time, end_time=end_time
                )
                return JSONResponse(content=response)

            except Exception as e:
                logger.error(
                    f"Failed to retrieve logs for device '{data.get('name', 'unknown')}' with id {data.get('id', 'unknown')}, "
                    f"measurement '{data.get('measurement', 'unknown')}': {e}"
                )
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.delete("/delete_logs")
        async def delete_logs_from_measurement(request: Request, authorization: str = Header(None)):
            """
            Endpoint to delete log data for a specific node from a device.

            Expects a JSON payload with the following fields:
                - name (str): The name of the device.
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
                if self.is_blocked(ip, "/delete_logs"):
                    return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

                self.check_authorization_token(authorization)
                data = await request.json()
                name = data.get("name")
                id = data.get("id")
                measurement = data.get("measurement")

                if not all([name, id, measurement]):
                    raise ValueError("Missing one or more required fields: 'name', 'id', 'measurement'.")

                device = self.device_manager.get_device(name, id)
                if not device:
                    raise KeyError(f"Device with name {name} and id {id} does not exist.")

                if not any(measurement == node.name for node in device.nodes):
                    raise KeyError(f"Node with name {measurement} does not exist in device {name} with id {id}.")

                result = self.timedb.delete_measurement_data(device_name=name, device_id=id, measurement=measurement)

                self.clean_failed_requests(ip, "/delete_logs")

                message = (
                    f"Successfully deleted logs for node '{measurement}' from device '{name}' (id {id})."
                    if result
                    else f"Failed to delete logs for node '{measurement}' from device '{name}' (id {id})."
                )
                return JSONResponse(content={"result": message})

            except Exception as e:
                self.increment_failed_requests(ip, "/delete_logs")
                logger.error(
                    f"Failed to delete logs for device '{data.get('name', 'unknown')}' with id {data.get('id', 'unknown')}, "
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
                if self.is_blocked(ip, "/delete_all_logs"):
                    return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

                self.check_authorization_token(authorization)
                data = await request.json()
                name = data.get("name")
                id = data.get("id")

                if not all([name, id]):
                    raise ValueError("Missing one or more required fields: 'name', 'id'.")

                result = self.timedb.delete_db(device_name=name, device_id=id)

                self.clean_failed_requests(ip, "/delete_all_logs")

                message = (
                    f"Successfully deleted all logs from device '{name}' (id {id})." if result else f"Failed to delete logs from device '{name}' (id {id})."
                )
                return JSONResponse(content={"result": message})

            except Exception as e:
                self.increment_failed_requests(ip, "/delete_all_logs")
                logger.error(f"Failed to delete all logs for device {data.get('name', 'unknown')} with id {data.get('id', 'unknown')}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})
