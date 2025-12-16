"""Utility functions."""

from .typing import FileLike

from pathlib import Path


def filelike_to_str(file: FileLike, encoding: str = "utf-8") -> str:
    """Convert a file-like input to a string.

    Parameters
    ----------
    file
        File-like input to be converted to a string.
    encoding
        Encoding used to decode the file if it is provided as bytes or Path.

    Returns
    -------
    file_content
        Content of the file as a string.
    """
    if isinstance(file, Path):
        return file.read_text(encoding=encoding)
    if isinstance(file, bytes):
        return file.decode(encoding=encoding)
    if isinstance(file, str):
        return file
    raise ValueError(
        "Parameter `file` expects either a string, bytes, or Path, but the type of input argument "
        f"was '{type(file)}'. Input was: {file}."
    )
