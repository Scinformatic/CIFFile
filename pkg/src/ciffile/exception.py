from __future__ import annotations

from typing import TYPE_CHECKING
from enum import Enum, auto


if TYPE_CHECKING:
    from typing import Literal
    from ciffile.typing import FileLike
    from .structure import CIFFile
    from .parser import CIFFileParseError


class CIFFileError(Exception):
    """Base exception for CIFFile errors."""
    def __init__(
        self,
        message: str,
    ):
        super().__init__(message)
        self.message = message
        return


class CIFFileReadErrorType(Enum):
    """Types of errors that may occur when reading a CIF file."""
    PARSING = auto()


class CIFFileReadError(CIFFileError):
    """Exception raised when a CIF file cannot be read."""
    def __init__(
        self,
        error_type: CIFFileReadErrorType,
        *,
        file: CIFFile,
        errors: list[CIFFileParseError],
        file_input: FileLike,
        variant: Literal["cif1", "mmcif"],
        encoding: str,
    ):
        self.error_type = error_type
        self.file = file
        self.errors = errors
        self.file_input = file_input
        self.variant = variant
        self.encoding = encoding
        error_handler = getattr(self, f"_msg_{error_type.name.lower()}")
        self.message = error_handler()
        super().__init__(message=self.message)
        return

    def _msg_parsing(self) -> str:
        return (
            f"Failed to parse CIF file ({len(self.errors)} errors encountered)."
        )
