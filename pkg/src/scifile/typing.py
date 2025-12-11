"""Type-hint definitions.

This module defines type-hints used throughout the package.
"""

from pathlib import Path
from typing import TypeAlias


FileLike: TypeAlias = str | bytes | Path
"""A file-like input.

- If a `pathlib.Path` is provided, it is interpreted as the path to a file.
- If `bytes` are provided, they are interpreted as the content of the file.
- If a `str` is provided, it is interpreted as the content of the file.
"""
