"""CIF file reader."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .parser import parse
from .structure import CIFFile
from .exception import CIFFileReadError, CIFFileReadErrorType

if TYPE_CHECKING:
    from .typing import FileLike
    from typing import Literal


def read(
    file: FileLike,
    *,
    variant: Literal["cif1", "mmcif"] = "mmcif",
    encoding: str = "utf-8",
    case_normalization: Literal["lower", "upper"] | None = "lower",
    raise_level: Literal[0, 1, 2] = 2,
    col_name_block: str = "block",
    col_name_frame: str = "frame",
    col_name_cat: str = "category",
    col_name_key: str = "keyword",
    col_name_values: str = "values",
    allow_duplicate_rows: bool = False,
) -> CIFFile:
    """Read a CIF file from content, path, or a file-like object.

    Parameters
    ----------
    file
        Content, path, or file-like object to read the CIF data from.
        - If an `os.PathLike` object is provided, it is interpreted as the path to the file.
        - If a string is provided, it is interpreted as the content of the file
          unless it is a valid existing file path,
          in which case it is treated as the path to the file.
        - If a bytes-like object is provided, it is interpreted as the content of the file.
        - If an object with a `read()` method is provided, it is treated as a file-like object.
    variant
        CIF variant to read; one of:
        - `"cif1"`: CIF version 1.1 format
        - `"mmcif"`: macromolecular CIF format (default)

        This affects parsing behavior and validation checks.
        For example, mmCIF data names must contain
        a category and keyword separated by a period (e.g., `_atom_site.Cartn_x`),
        while CIF version 1.1 data names do not have such restrictions.
    encoding
        Encoding used to decode the file if it is provided as bytes or Path.
    raise_level
        Level of parsing errors to raise as exceptions; one of:
        - `0`: Raise all errors and warnings.
        - `1`: Raise all errors, ignore warnings.
        - `2`: Only raise fatal errors (default).
    col_name_block
        Name of the column in the resulting CIFFile
        that will contain the block codes (data block names).
    col_name_frame
        Name of the column in the resulting CIFFile
        that will contain the frame codes (save frame names within blocks).
    col_name_cat
        Name of the column in the resulting CIFFile
        that will contain the category of each data item.
    col_name_key
        Name of the column in the resulting CIFFile
        that will contain the keyword of each data item.
    col_name_values
        Name of the column in the resulting CIFFile
        that will contain the values of each data item.
    allow_duplicate_rows
        Whether to permit duplicate rows (same block, frame, category, key)
        and aggregate them during validation. Defaults to False.

    Returns
    -------
    CIFFile
        The parsed CIF file.

    Raises
    ------
    CIFFileReadError
        If parsing errors occur that meet or exceed the specified `raise_level`.
    """
    columns, parsing_errors = parse(
        file=file,
        variant=variant,
        encoding=encoding,
        case_normalization=case_normalization,
        raise_level=raise_level
    )
    column_name_map = {
        "block": col_name_block,
        "frame": col_name_frame,
        "category": col_name_cat,
        "keyword": col_name_key,
        "values": col_name_values,
    }
    cif = CIFFile(
        content={column_name_map[k]: v for k, v in columns.items()},
        variant=variant,
        validate=True,
        col_name_block=col_name_block,
        col_name_frame=col_name_frame,
        col_name_cat=col_name_cat,
        col_name_key=col_name_key,
        col_name_values=col_name_values,
        allow_duplicate_rows=allow_duplicate_rows,
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
