"""Utility functions for tap functionality"""

from typing import Optional


def replicate_pk_at_root(data: dict, jsonpaths: Optional[list]) -> dict:
    """Replicates the indicated values as root properties.

    Arguments:
        data: The json object to be modified.
        jsonpaths: A list of json paths in format 'level1.level2.etc' that
            indicate which values are to be replicated as a root level property.

    Returns:
        A modified json record with the duplicate values as root level primary keys.
    """
    if not jsonpaths:
        return data

    pk_str = ""
    new_data = data.copy()
    for path in jsonpaths:
        levels = path.split(".")

        # recursively build pk value
        val = data.copy()
        for i, level in enumerate(levels):
            val = val[level]
            if i == len(levels) - 1:
                pk_str += ":" + str(val) if pk_str else str(val)

    new_data["_sdc_primary_key"] = pk_str

    return new_data
