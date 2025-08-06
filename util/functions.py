###########EXTERNAL IMPORTS############

from cryptography.fernet import Fernet
from datetime import datetime
from typing import Dict
import base64
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


def process_and_save_image(image: UploadFile, device_id: int, min_px: int, directory: str, image_extension: str = "png") -> None:
    """
    Processes and saves an uploaded image with resizing and optimization.

    The function:
    1. Validates the uploaded file is an image
    2. Resizes the image maintaining aspect ratio with specified minimum dimension
    3. Saves as optimized image format (preserves transparency for PNG)
    4. Saves to specified directory with device ID as filename

    Args:
        image (UploadFile): The uploaded image file from FastAPI
        device_id (int): The device ID to use as filename
        min_px (int): The minimum number of pixels for the smaller dimension of the resized image
        directory (str): The directory path where the image should be saved
        image_extension (str, optional): The file extension for the saved image. Defaults to "png".

    Raises:
        ValueError: If the file is not a valid image or processing fails
    """

    # Validate content type
    if not image.content_type or not image.content_type.startswith('image/'):
        raise ValueError(f"Invalid file type: {image.content_type}. Only image files are allowed.")

    os.makedirs(directory, exist_ok=True)

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
            png_path = os.path.join(directory, f"{device_id}.{image_extension}")
            resized_img.save(png_path, format='PNG', optimize=True)

    except Exception as e:
        raise ValueError(f"Failed to process image: {str(e)}")
    finally:
        image.file.seek(0)


def get_device_image(
    device_id: int, default_image_str: str, directory: str, image_extension: str = "png", decode_type: str = "utf-8", force_default: bool = False
) -> Dict[str, str]:
    """
    Retrieves a device image or falls back to a default image, encoded as base64.

    This function looks for a device-specific image file first, and if not found,
    falls back to a default image. The image is then base64-encoded for JSON transport.

    Args:
        device_id (int): The unique identifier of the device to get the image for.
        default_image_str (str): The filename (without extension) of the default image to use as fallback.
        directory (str): The directory path where images are stored (should end with '/').
        image_extension (str, optional): The file extension of the image files. Defaults to "png".
        decode_type (str, optional): The encoding type for base64 decoding. Defaults to "utf-8".
        force_default (bool, optional): If True, always use the default image regardless of whether
                                       a device-specific image exists. Defaults to False.

    Returns:
        Dict[str, str]: A dictionary containing:
            - "data": Base64-encoded image data
            - "type": MIME type of the image (e.g., "image/png")
            - "filename": The actual filename used (device-specific or default)

    Raises:
        ValueError: If neither the device-specific image nor the default image exists.
    """

    image_path = f"{directory}{device_id}.{image_extension}"
    default_image_path = f"{directory}{default_image_str}.{image_extension}"
    image_type = f"image/{image_extension}"

    output_dict: Dict[str, str] = dict()

    if os.path.exists(image_path) and not force_default:
        with open(image_path, "rb") as image_file:
            image_data = base64.b64encode(image_file.read()).decode(decode_type)
            output_dict = {"data": image_data, "type": image_type, "filename": f"{device_id}.{image_extension}"}
    elif os.path.exists(default_image_path):
        with open(default_image_path, "rb") as image_file:
            image_data = base64.b64encode(image_file.read()).decode(decode_type)
            output_dict = {"data": image_data, "type": image_type, "filename": f"{default_image_str}.{image_extension}"}
    else:
        raise ValueError(f"No device image or default image found for device with id {device_id}")

    return output_dict


def delete_device_image(device_id: int, directory: str, image_extension: str = "png") -> bool:
    """
    Deletes a device-specific image file from the filesystem.

    This function removes the image file associated with a specific device ID.
    It does not delete the default image, only device-specific images.

    Args:
        device_id (int): The unique identifier of the device whose image should be deleted.
        directory (str): The directory path where images are stored (should end with '/').
        image_extension (str, optional): The file extension of the image file. Defaults to "png".

    Returns:
        bool: True if the image was successfully deleted or didn't exist, False if deletion failed.
    """

    image_path = f"{directory}{device_id}.{image_extension}"

    try:
        if os.path.exists(image_path):
            os.remove(image_path)
            return True
        else:
            return True
    except Exception:
        return False
