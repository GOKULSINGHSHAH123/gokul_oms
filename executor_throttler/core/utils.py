"""
Utility functions for data parsing and transformation.

Provides helper functions for parsing Redis data structures and converting byte
strings to appropriate Python types (int, float, str).
"""


def parse_redis_dict_values(dict_):
    """
    Parse Redis dictionary by decoding bytes and converting to native Python types.

    Converts byte strings returned by Redis to UTF-8 strings, then attempts to
    convert numeric strings to int or float types for type-safe operations.

    Args:
        dict_: Dictionary with bytes or string keys and values from Redis

    Returns:
        Dictionary with decoded string keys/values and inferred numeric types.
        Non-numeric strings remain as strings.
    """
    return_dict = {}

    for key, value in dict_.items():
        try:
            key = key.decode('utf-8')
            value = value.decode('utf-8')
        except AttributeError:
            pass

        return_dict[key] = value
        try:
            if value.replace('.', '', 1).isdigit():
                return_dict[key] = float(value) if '.' in value else int(value)
        except (ValueError, TypeError, AttributeError):
            continue

    return return_dict

