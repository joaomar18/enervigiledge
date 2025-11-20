###########EXTERNAL IMPORTS############

#######################################

#############LOCAL IMPORTS#############

#######################################


def validate_password(password: str) -> bool:
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
