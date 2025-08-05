###########EXTERNAL IMPORTS############

from cryptography.fernet import Fernet
from datetime import datetime
import random
import time
import os
from fastapi import UploadFile
from PIL import Image
import io

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


def remove_phase_string(name: str) -> str:
    """
    Removes the phase or total prefix (e.g., 'l1_', 'l2_', 'l3_', 'total_', 'l1_l2_', etc.) from a node name if present.

    Args:
        name (str): The name of the node.

    Returns:
        str: The node name without the phase prefix.
    """

    parts = name.split("_")

    # Handle common prefixes
    if parts[0] in {"l1", "l2", "l3", "total"}:
        # Check for line-to-line voltages
        if len(parts) > 1 and parts[1] in {"l1", "l2", "l3"}:
            return "_".join(parts[2:])
        return "_".join(parts[1:])

    return name


def generate_random_number(min: int = 0, max: int = 100000) -> int:
    """
    Generates a random integer between the specified range.

    Args:
        min (int): Minimum value (inclusive). Defaults to 0.
        max (int): Maximum value (inclusive). Defaults to 100000.

    Returns:
        int: Randomly generated integer.
    """

    return random.randint(min, max)


def get_current_date() -> datetime:
    """
    Returns the current date and time.

    Returns:
        datetime: The current timestamp.
    """

    return datetime.fromtimestamp(time.time())


def subtract_datetime_mins(date_time_01: datetime, date_time_02: datetime) -> int:
    """
    Calculates the difference in minutes between two datetime objects, ignoring the date.

    Args:
        date_time_01 (datetime): First datetime.
        date_time_02 (datetime): Second datetime.

    Returns:
        int: Difference in minutes between the two times.
    """

    minutes_01 = date_time_01.minute + (date_time_01.hour * 60)
    minutes_02 = date_time_02.minute + (date_time_02.hour * 60)
    return minutes_01 - minutes_02


def process_and_save_image(image: UploadFile, device_id: int, min_px: int) -> None:
    """
    Processes and saves an uploaded image with resizing and optimization.

    The function:
    1. Validates the uploaded file is an image
    2. Resizes the image maintaining aspect ratio (200px minimum dimension)
    3. Saves as PNG with optimization (preserves transparency)
    4. Saves to db/device_img/ directory

    Args:
        image (UploadFile): The uploaded image file from FastAPI
        device_id (int): The device ID to use as filename
        min_px (int); The minimum number of pixels in a direction of the image to be saved

    Raises:
        ValueError: If the file is not a valid image or processing fails
    """

    # Validate content type
    if not image.content_type or not image.content_type.startswith('image/'):
        raise ValueError(f"Invalid file type: {image.content_type}. Only image files are allowed.")

    save_dir = "db/device_img"
    os.makedirs(save_dir, exist_ok=True)

    try:
        image_data = image.file.read()

        with Image.open(io.BytesIO(image_data)) as img:
            if img.mode == 'P':
                img = img.convert('RGBA')

            original_width, original_height = img.size

            if original_width <= original_height:
                new_width = min_px
                new_height = int((original_height * min_px) / original_width)
            else:
                new_height = min_px
                new_width = int((original_width * min_px) / original_height)

            resized_img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            png_path = os.path.join(save_dir, f"{device_id}.png")
            resized_img.save(png_path, format='PNG', optimize=True)

    except Exception as e:
        raise ValueError(f"Failed to process image: {str(e)}")
    finally:
        image.file.seek(0)
