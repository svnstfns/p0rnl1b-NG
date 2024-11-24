# Gemeinsame Hilfsfunktionen und wiederverwendbare Module.
# utils.py
"""
Utility Functions

Provides helper functions for common tasks such as checksum computation
and file path building.
"""

import hashlib
import logging

logger = logging.getLogger(__name__)

def compute_file_checksum(file_path):
    """
    Computes the SHA256 checksum of a file.

    Args:
        file_path (str): Path to the file.

    Returns:
        str: SHA256 checksum of the file, or None if an error occurs.
    """
    hash_algorithm = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_algorithm.update(chunk)
    except IOError as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None
    return hash_algorithm.hexdigest()

def build_file_path(dataset, path, volume_mapping):
    """
    Constructs the full file path based on dataset and path.

    Args:
        dataset (str): Dataset name.
        path (str): File path within the dataset.
        volume_mapping (dict): Mapping of datasets to volume paths.

    Returns:
        str: Full file path, or None if the dataset is not found in the mapping.
    """
    if dataset in volume_mapping:
        mount_point = volume_mapping[dataset]
        return path.replace(mount_point, volume_mapping[dataset])
    else:
        logger.error(f"Dataset {dataset} not found in volume mapping.")
        return None
