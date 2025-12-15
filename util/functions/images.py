###########EXTERNAL IMPORTS############

from typing import Dict, Optional
import base64
import os
from starlette.datastructures import UploadFile
from PIL import Image
import shutil
import io

#######################################

#############LOCAL IMPORTS#############

#######################################


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


def process_and_save_image(image: UploadFile, device_id: int, min_px: int, directory: str, bin_directory: Optional[str] = None, image_extension: str = "png") -> bool:
    """
    Processes and saves an uploaded image with resizing and optimization.

    This function validates the uploaded file type, resizes the image while
    preserving its aspect ratio based on a minimum dimension, and saves the
    optimized image to disk using the device ID as filename. If a bin directory
    is provided and an existing image is found, the existing image is moved
    to the bin directory before saving the new image.

    Args:
        image (UploadFile): The uploaded image file provided by FastAPI.
        device_id (int): The device ID used as the output image filename.
        min_px (int): Minimum pixel size for the smallest image dimension.
        directory (str): Target directory where the new image will be saved.
        bin_directory (Optional[str]): Directory where an existing image is
            archived before replacement. If None, existing images are
            overwritten directly.
        image_extension (str, optional): File extension of the saved image.
            Defaults to "png".

    Returns:
        bool: True if the image was successfully processed and saved, False
        if validation fails or an error occurs during processing.

    Note:
        - If `bin_directory` is provided, existing images are archived before
          being replaced.
        - This function does not perform rollback or cleanup of archived images.
        - The uploaded file stream is reset to the beginning after processing,
          allowing the image to be reused if needed.
    """

    # Validate content type
    if not image.content_type or not image.content_type.startswith('image/'):
        return False

    
    success = False

    try:
        os.makedirs(directory, exist_ok=True)
        if bin_directory:
            os.makedirs(bin_directory, exist_ok=True)
        
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
            image_path = os.path.join(directory, f"{device_id}.{image_extension}")
            if bin_directory:
                bin_path = os.path.join(bin_directory, f"{device_id}.{image_extension}")
                if os.path.exists(image_path):
                    shutil.move(image_path, bin_path)
            
            resized_img.save(image_path, format='PNG', optimize=True)
            success = True

    except Exception as e:
        success = False
    finally:
        image.file.seek(0)
    
    return success


def delete_device_image(device_id: int, directory: str, bin_directory: Optional[str] = None, image_extension: str = "png") -> bool:
    """
    Removes a device-specific image from the filesystem.

    If a bin directory is provided, the image is first moved to the bin
    directory before removal. This function is intended for cleanup and
    rollback scenarios and does not raise exceptions if the image does not exist.

    Args:
        device_id (int): The unique identifier of the device.
        directory (str): Directory where the active device images are stored.
        bin_directory (Optional[str]): Directory where the image is archived
            before deletion. If None, the image is removed directly.
        image_extension (str, optional): Image file extension. Defaults to "png".

    Returns:
        bool: True if the image was successfully removed or did not exist,
        False if an error occurred during the operation.
    """

    image_path = f"{directory}{device_id}.{image_extension}"

    try:
        if os.path.exists(image_path):
            if bin_directory:
                bin_path = os.path.join(bin_directory, f"{device_id}.{image_extension}")
                shutil.move(image_path, bin_path)
                
            os.remove(image_path)
            return True
        else:
            return True
    except Exception:
        return False


def rollback_image(device_id: int, directory: str, bin_directory: str, image_extension: str = "png") -> bool:
    """
    Restores a previously archived image from the bin directory to the main directory.

    If the device image is missing from the main directory, it is restored from the
    bin directory. The original image in the main directory is deleted before restoration.

    Args:
        device_id (int): The device ID associated with the image.
        directory (str): Directory where the image should be restored.
        bin_directory (str): Directory containing the archived image.
        image_extension (str, optional): File extension of the image. Defaults to "png".

    Returns:
        bool: True if the image was successfully restored or no action was performed, False if an error occurred.
    """
    
    image_path = f"{directory}{device_id}.{image_extension}"
    bin_path = f"{bin_directory}{device_id}.{image_extension}"
    
    try:
        if os.path.exists(directory) and not os.path.exists(image_path) and os.path.exists(bin_path):
            delete_device_image(device_id=device_id, directory=directory, bin_directory=None, image_extension=image_extension)
            shutil.move(bin_path, image_path)
            return True
        else:
            return True
    except Exception:
        return False
    

def flush_bin_images(bin_directory: str) -> bool:
    """
    Deletes all files and subdirectories in the specified bin directory.
    This function removes only the contents of the directory, leaving the directory itself intact.

    Args:
        bin_directory (str): The directory whose contents should be deleted.

    Returns:
        bool: True if the contents were successfully deleted, False if an error occurred or the directory does not exist.
    """

    try:
        if os.path.exists(bin_directory):
            for filename in os.listdir(bin_directory):
                file_path = os.path.join(bin_directory, filename)
    
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            
            return True
        else:
            return True
    except Exception:
        return False
