###########EXTERNAL IMPORTS############

from cryptography.fernet import Fernet

#######################################

#############LOCAL IMPORTS#############

#######################################


def decrypt_password(password_encrypted: str, key: str) -> str:
    """
    Decrypts an encrypted password using the given Fernet key.

    Args:
        password_encrypted (str): The encrypted password string.
        key (str): The encryption key used to decrypt.

    Returns:
        str: The decrypted password.
    """

    return Fernet(key).decrypt(password_encrypted.encode()).decode()
