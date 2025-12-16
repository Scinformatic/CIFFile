"""Read CIF files."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .parser import parse
from .structure import CIFFile, file as _create_cif_file
from .exception import CIFFileReadError, CIFFileReadErrorType

if TYPE_CHECKING:
    from scifile.typing import FileLike
    from typing import Literal


def read(
    file: FileLike,
    *,
    variant: Literal["cif1", "mmcif"] = "mmcif",
    encoding: str = "utf-8",
    raise_level: Literal[0, 1, 2] = 2,
    col_name_block: str = "block",
    col_name_frame: str = "frame",
    col_name_cat: str = "category",
    col_name_key: str = "keyword",
    col_name_values: str = "values",
) -> CIFFile:
    columns, parsing_errors = parse(file=file, variant=variant, encoding=encoding, raise_level=raise_level)
    column_name_map = {
        "block": col_name_block,
        "frame": col_name_frame,
        "category": col_name_cat,
        "keyword": col_name_key,
        "values": col_name_values,
    }
    cif = _create_cif_file(
        content={column_name_map[k]: v for k, v in columns.items()},
        variant=variant,
        validate=True,
        col_name_block=col_name_block,
        col_name_frame=col_name_frame,
        col_name_cat=col_name_cat,
        col_name_key=col_name_key,
        col_name_values=col_name_values,
    )
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
