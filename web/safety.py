###########EXTERNAL IMPORTS############

import logging
import json
from fastapi import Request
from datetime import datetime, timezone, timedelta
from typing import Dict, Set, Optional, Any
from dataclasses import dataclass
import jwt

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager

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
