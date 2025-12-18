###########EXTERNAL IMPORTS############

import os
import logging
import json
from fastapi import Request
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any, Tuple
from dataclasses import dataclass
import jwt
import secrets
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, InvalidHashError

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
import web.validation as validation
import util.functions.web as web_util
import web.exceptions as api_exception

#######################################

LoggerManager.get_logger(__name__).setLevel(logging.DEBUG)


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
        failed_requests (Dict[str, Dict[str, RequestsSafety]]): Tracks failed attempts per client identifier and endpoint.
                                                                Client identifier can be JWT token (for authenticated requests)
                                                                or IP+User-Agent hash (for unauthenticated requests).
        active_tokens (Dict[str, LoginToken]): Stores active JWT tokens by token value, including session metadata like IP,
                                               auto-login status, and keep-alive timestamp. Each token represents a unique session.
        ph (PasswordHasher): Argon2 password hasher instance for secure password hashing and verification.
    """

    USER_CONFIG_PATH = str("user_config.json")
    MAX_REQUEST_ATTEMPTS = 5
    BLOCK_TIME = timedelta(minutes=15)

    def __init__(self):
        self.failed_requests: Dict[str, Dict[str, RequestsSafety]] = {}
        self.active_tokens: Dict[str, LoginToken] = {}
        self.ph = PasswordHasher()

    async def create_user_configuration(self, username: str, password: str) -> None:
        """
        Create the initial user authentication configuration.

        Initializes the user configuration by validating the provided credentials,
        generating a password hash and JWT secret, and persisting them to disk.
        This operation is allowed only once.

        Args:
            username: Username to store in the authentication configuration.
            password: Plain-text password to validate and hash.

        Raises:
            UserConfigurationExists: If a user configuration already exists.
            InvalidCredentials: If the provided password does not meet validation
                requirements.
        """

        if not validation.validate_password(password):
            raise api_exception.InvalidCredentials(api_exception.Errors.AUTH.INVALID_PASSWORD)

        if os.path.exists(HTTPSafety.USER_CONFIG_PATH):
            raise api_exception.UserConfigurationExists(api_exception.Errors.AUTH.USER_CONFIG_EXISTS)

        hashed_password = self.ph.hash(password)
        jwt_secret = secrets.token_hex(32)

        config = {"username": username, "password_hash": hashed_password, "jwt_secret": jwt_secret}

        with open(HTTPSafety.USER_CONFIG_PATH, "w") as file:
            json.dump(config, file, indent=4)

    async def change_user_password(self, username: str, old_password: str, new_password: str, confirm_new_password: str) -> None:
        """
        Update the stored user password after verifying existing credentials.

        Validates the current credentials, enforces password rules, and replaces
        the stored password hash with a new one. The user configuration must
        already exist and be valid.

        Args:
            username: Username of the account to update.
            old_password: Current password used for verification.
            new_password: New password to store.
            confirm_new_password: Confirmation of the new password.

        Raises:
            InvalidCredentials: If authentication fails, passwords do not match,
                or the new password does not meet validation requirements.
            UserConfigurationNotFound: If the user configuration does not exist.
            UserConfigCorrupted: If the stored configuration is invalid or incomplete.
        """

        if new_password != confirm_new_password:
            raise api_exception.InvalidCredentials(api_exception.Errors.AUTH.PASSWORD_MISMATCH)

        if not validation.validate_password(new_password):
            raise api_exception.InvalidCredentials(api_exception.Errors.AUTH.INVALID_NEW_PASSWORD)

        # Checks if configuration file exists
        if not os.path.exists(HTTPSafety.USER_CONFIG_PATH):
            raise api_exception.UserConfigurationNotFound(api_exception.Errors.AUTH.USER_CONFIG_NOT_FOUND)

        # Obtain user configuration
        with open(HTTPSafety.USER_CONFIG_PATH, "r") as file:
            config: Dict[str, Any] = json.load(file)

        stored_username: Optional[str] = config.get("username")
        stored_hash: Optional[str] = config.get("password_hash")
        if stored_username is None or stored_hash is None or not isinstance(stored_username, str) or not isinstance(stored_hash, str):
            raise api_exception.UserConfigCorrupted(api_exception.Errors.AUTH.USER_CONFIG_CORRUPT)

        if username != stored_username:
            raise api_exception.InvalidCredentials(api_exception.Errors.AUTH.INVALID_CREDENTIALS)

        try:
            self.ph.verify(stored_hash, old_password)
        except (VerifyMismatchError, InvalidHashError):
            raise api_exception.InvalidCredentials(api_exception.Errors.AUTH.INVALID_CREDENTIALS)

        # Generate new hash and update config
        new_hash = self.ph.hash(new_password)
        config["password_hash"] = new_hash

        with open(HTTPSafety.USER_CONFIG_PATH, "w") as file:
            json.dump(config, file, indent=4)

    def get_client_identifier(self, request: Request) -> str:
        """
        Get a unique identifier for the client making the request.

        For authenticated requests, uses the JWT token.
        For unauthenticated requests, uses a hash of IP + User-Agent.

        Args:
            request: FastAPI request object

        Returns:
            str: Unique client identifier
        """
        # Try to get JWT token first
        token = None
        authorization = request.headers.get("authorization")
        if authorization and authorization.startswith("Bearer "):
            token = authorization.split(" ")[1]
        else:
            token = request.cookies.get("token")

        if token and token in self.active_tokens:
            return token

        # Fall back to IP + User-Agent fingerprint for unauthenticated requests
        ip = web_util.get_ip_address(request)
        user_agent = request.headers.get("user-agent", "")
        fingerprint = f"{ip}:{hash(user_agent)}"
        return fingerprint

    async def create_jwt_token(self, username: str, password: str, auto_login: bool, request: Request) -> Tuple[str, str]:
        """
        Authenticate user credentials and issue a JWT access token.

        Validates the provided username and password against the stored user
        configuration. On successful authentication, generates and signs a JWT
        token, registers the active session with the client IP, and applies the
        requested auto-login behavior.

        On authentication failure, remaining login attempts and unlock timing
        information may be included in the raised exception details.

        Args:
            username: Username to authenticate.
            password: Plain-text password to verify.
            auto_login: Whether the session should persist across restarts.
            request: Incoming request, used for client identification and rate tracking.

        Returns:
            Tuple[str, str]: The authenticated username and the generated JWT token.

        Raises:
            InvalidCredentials: If the username or password is incorrect.
            UserConfigurationNotFound: If the user configuration does not exist.
            UserConfigCorrupted: If the stored user configuration is invalid or unreadable.
        """

        # Checks if configuration file exists
        if not os.path.exists(HTTPSafety.USER_CONFIG_PATH):
            raise api_exception.UserConfigurationNotFound(api_exception.Errors.AUTH.USER_CONFIG_NOT_FOUND)

        # Obtain user configuration
        with open(HTTPSafety.USER_CONFIG_PATH, "r") as file:
            config: Dict[str, Any] = json.load(file)

        stored_username: Optional[str] = config.get("username")
        stored_hash: Optional[str] = config.get("password_hash")
        if stored_username is None or stored_hash is None or not isinstance(stored_username, str) or not isinstance(stored_hash, str):
            raise api_exception.UserConfigCorrupted(api_exception.Errors.AUTH.USER_CONFIG_CORRUPT)

        # Verify credentials
        try:
            self.ph.verify(stored_hash, password)
            password_correct = True
        except (VerifyMismatchError, InvalidHashError):
            password_correct = False

        if username != config.get("username") or not validation.validate_password(password) or not password_correct:
            raise api_exception.InvalidCredentials(api_exception.Errors.AUTH.INVALID_CREDENTIALS)

        # Create token and return it
        token_payload = {"user": username, "iat": datetime.now(timezone.utc).timestamp()}
        token = jwt.encode(token_payload, config["jwt_secret"], algorithm="HS256")
        self.active_tokens[token] = LoginToken(
            token=token, user=username, ip=web_util.get_ip_address(request), auto_login=auto_login, keep_session_until=None
        )
        return (username, token)

    async def update_jwt_token(self, request: Request) -> Tuple[str, str]:
        """
        Refreshes an existing JWT access token while preserving session settings.

        This method validates the current authentication token, issues a new JWT
        with an updated issuance timestamp, and retains session attributes such
        as auto-login state.

        Args:
            request (Request): HTTP request containing the current authentication
                token in headers or cookies.

        Returns:
            Tuple[str, str]: A tuple containing the authenticated username and the
            refreshed JWT token.
        """

        username, token, jwt_secret = self.check_authorization_token(request)
        new_payload = {"user": username, "iat": datetime.now(timezone.utc).timestamp()}
        new_token = jwt.encode(new_payload, jwt_secret, algorithm="HS256")

        # Get auto_login status from current session and update token
        current_auto_login = self.active_tokens[token].auto_login
        del self.active_tokens[token]
        self.active_tokens[new_token] = LoginToken(
            token=new_token, user=username, ip=web_util.get_ip_address(request), auto_login=current_auto_login, keep_session_until=None
        )
        return (username, new_token)

    async def delete_jwt_token(self, request: Request) -> None:
        """
        Invalidates the current JWT token and removes the associated active session.

        This method validates the authentication token provided in the request and,
        if valid, deletes it from the active session store.

        Args:
            request (Request): HTTP request containing the authentication token
                in headers or cookies.

        Returns:
            None
        """

        username, token, jwt_secret = self.check_authorization_token(request)
        del self.active_tokens[token]

    def check_authorization_token(self, request: Request) -> Tuple[str, str, str]:
        """
        Validates the authentication token and returns authorization details.

        This method extracts the JWT token from the request headers or cookies,
        verifies its signature and payload, checks that the token corresponds to
        an active session, and validates the associated user and client context.

        Args:
            request (Request): HTTP request containing the authentication token
                in the Authorization header or cookies.

        Returns:
            Tuple[str, str, str]: A tuple containing the authenticated username,
            the JWT token string, and the JWT secret used for verification.

        Raises:
            InvalidCredentials: If the authentication token is missing, invalid,
                expired, revoked, or does not match the active session or request
                context.
            UserConfigurationNotFound: If the user configuration required for token
                validation does not exist.
        """

        token: str | None = None
        authorization = request.headers.get("authorization")

        if authorization:
            if authorization.startswith("Bearer "):
                token = authorization.split(" ")[1]
        else:
            token = request.cookies.get("token")

        if not token:
            raise api_exception.TokenNotInRequest(api_exception.Errors.AUTH.TOKEN_MISSING)

        # Checks if configuration file exists
        if not os.path.exists(HTTPSafety.USER_CONFIG_PATH):
            raise api_exception.UserConfigurationNotFound(api_exception.Errors.AUTH.USER_CONFIG_NOT_FOUND)

        # Obtain user configuration
        with open(HTTPSafety.USER_CONFIG_PATH, "r") as file:
            config: Dict[str, Any] = json.load(file)

        payload: Dict[str, Any] = jwt.decode(token, config["jwt_secret"], algorithms=["HS256"])
        username: str = payload["user"]

        # Check if token exists in active tokens and matches the token in the request
        stored_token = self.active_tokens.get(token)

        if not stored_token or stored_token.token != token:
            raise api_exception.TokenInRequestInvalid(api_exception.Errors.AUTH.INVALID_TOKEN)

        if stored_token.user != username or stored_token.ip != web_util.get_ip_address(request):
            raise api_exception.TokenInRequestInvalid(api_exception.Errors.AUTH.INVALID_TOKEN)

        return (username, token, str(config["jwt_secret"]))

    def is_blocked(self, request: Request) -> bool:
        """
        Checks if client is currently blocked for an endpoint due to failed attempts.

        Args:
            request: Request to identify the client

        Returns:
            bool: True if client is blocked, False otherwise
        """

        client_id = self.get_client_identifier(request)
        client_attempts = self.failed_requests.get(client_id, {})
        attempt: Optional[RequestsSafety] = client_attempts.get(web_util.get_api_url(request))

        if not attempt:
            return False

        now = datetime.now(timezone.utc)

        if attempt.blocked_until and now < attempt.blocked_until:
            return True

        if attempt.last_attempt_time and now - attempt.last_attempt_time > HTTPSafety.BLOCK_TIME:
            self.clean_failed_requests(request, web_util.get_api_url(request))

        return False

    def get_unlocked_date(self, request: Request) -> Optional[str]:
        """
        Returns ISO date when client will be unblocked for current endpoint.
        If not blocked returns None.
        """

        client_id = self.get_client_identifier(request)
        failed_requests = self.failed_requests.get(client_id, {}).get(web_util.get_api_url(request))
        if failed_requests is not None:
            return failed_requests.blocked_until.isoformat() if failed_requests.blocked_until is not None else None
        return None

    def get_remaining_requests(self, request: Request) -> int:
        """Returns number of remaining requests before client gets blocked."""

        client_id = self.get_client_identifier(request)
        failed_requests = self.failed_requests.get(client_id, {}).get(web_util.get_api_url(request))
        requests_count = failed_requests.count if failed_requests is not None else 0
        remaining_requests: int = HTTPSafety.MAX_REQUEST_ATTEMPTS - requests_count if requests_count else HTTPSafety.MAX_REQUEST_ATTEMPTS
        return remaining_requests

    def clean_failed_requests(self, request: Request, endpoint: str) -> None:
        """
        Removes failed request tracking for a client and endpoint.

        Args:
            request: Request to identify the client
            endpoint: Endpoint path to clear tracking for
        """

        client_id = self.get_client_identifier(request)

        if client_id in self.failed_requests and endpoint in self.failed_requests[client_id]:
            del self.failed_requests[client_id][endpoint]

        if client_id in self.failed_requests and not self.failed_requests[client_id]:
            del self.failed_requests[client_id]

    def increment_failed_requests(self, request: Request, endpoint: str) -> None:
        """
        Increments failed request counter and blocks client if limit exceeded.

        Args:
            request: Request to identify the client
            endpoint: Endpoint path for tracking
        """

        logger = LoggerManager.get_logger(__name__)
        now = datetime.now(timezone.utc)

        client_id = self.get_client_identifier(request)
        client_record: Dict[str, RequestsSafety] = self.failed_requests.setdefault(client_id, {})

        record: RequestsSafety = client_record.get(endpoint, RequestsSafety(endpoint, 0, None, None))

        # Reset record if block expired
        if record.blocked_until and now >= record.blocked_until:
            record = RequestsSafety(endpoint, 0, None, None)

        record.count += 1
        record.last_attempt_time = now

        if record.count >= HTTPSafety.MAX_REQUEST_ATTEMPTS:
            record.blocked_until = now + HTTPSafety.BLOCK_TIME
            logger.warning(f"Client {client_id} blocked from {endpoint} for {HTTPSafety.BLOCK_TIME}.")

        client_record[endpoint] = record
