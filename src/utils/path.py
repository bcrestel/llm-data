import subprocess
from pathlib import Path
from typing import List


def list_intermediate_directories(source: str, destination: str) -> List[Path]:
    """
    List intermediate directories from source to destination

    Args:
        source (str): starting directory
        destination (str): end directory

    Returns:
        List[Path]: List of all intermediate directories from source to destination
    """
    source_path = Path(source).resolve()
    destination_path = Path(destination).resolve()
    intermediate_path = []

    # Ensure source is a parent of destination
    if not destination_path.is_relative_to(source_path):
        print("The source path is not a parent of the destination path.")
        return []

    intermediate_path.append(destination_path)
    # Iterate from destination to source
    for parent in destination_path.parents:
        intermediate_path.append(parent)
        if parent == source_path:
            break

    return intermediate_path[::-1]


def change_permission_single_file(file_path: str, permission) -> None:
    file_path = Path(file_path).resolve()
    file_path.chmod(permission)


def chmod_from_top_to_bottom(source: str, destination: str, permission) -> None:
    source_path = Path(source).resolve()
    destination_path = Path(destination).resolve()
    top = (
        source_path
        if destination_path.is_relative_to(source_path)
        else destination_path
    )
    bottom = (
        destination_path
        if destination_path.is_relative_to(source_path)
        else source_path
    )
    list_paths = list_intermediate_directories(source=top, destination=bottom)
    for path in list_paths:
        path.chmod(permission)


def chmod_from_bottom_to_top(source: str, destination: str, permission) -> None:
    source_path = Path(source).resolve()
    destination_path = Path(destination).resolve()
    top = (
        source_path
        if destination_path.is_relative_to(source_path)
        else destination_path
    )
    bottom = (
        destination_path
        if destination_path.is_relative_to(source_path)
        else source_path
    )
    list_paths = list_intermediate_directories(source=top, destination=bottom)[::-1]
    for path in list_paths:
        path.chmod(permission)


def get_shasum(file_name: str) -> str:
    result = subprocess.run(
        ["shasum", file_name], capture_output=True, text=True, check=True
    )
    sha_checksum = result.stdout.split()[0]
    return sha_checksum
