"""Read CIF files."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .parser import parse
from .structure import CIFFile
from .exception import CIFFileReadError, CIFFileReadErrorType

if TYPE_CHECKING:
    from scifile.typing import FileLike
    from typing import Literal


def read(
    file: FileLike,
    *,
    variant: Literal["cif1", "mmcif"] = "mmcif",
    encoding: str = "utf-8",
) -> CIFFile:
    columns, parsing_errors = parse(file=file, variant=variant, encoding=encoding)
    cif = CIFFile(content=columns)
    if parsing_errors:
        raise CIFFileReadError(
            error_type=CIFFileReadErrorType.PARSING,
            file=cif,
            errors=parsing_errors,
            file_input=file,
            variant=variant,
            encoding=encoding,
        )
    return cif
