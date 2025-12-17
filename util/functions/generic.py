###########EXTERNAL IMPORTS############

import random

#######################################

#############LOCAL IMPORTS#############

#######################################


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
