"""CIF file validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from .ddl2 import validator as ddl2_validator, dictionary as ddl2_dictionary


if TYPE_CHECKING:
    from typing import Literal, Sequence
    from ciffile.structure import CIFFile, CIFBlock
    from ._base import CIFFileValidator
    from .ddl2 import DDL2Generator, DDL2Validator


__all__ = [
    "dictionary",
    "validator",
]


def dictionary(
    file: CIFFile | CIFBlock,
    *,
    variant: str = "ddl2",
) -> dict:
    """Create a CIF file validator from a CIF dictionary.

    Parameters
    ----------
    file
        CIF dictionary file.
    variant
        Dictionary definition language variant.
        Currently, only "ddl2" is supported.

    Returns
    -------
    CIFFileValidator
        CIF file validator instance.
    """
    if variant == "ddl2":
        generator = ddl2_dictionary
    else:
        raise ValueError(f"Unsupported dictionary variant: {variant!r}")
    return generator(file)


def validator(
    dictionary: dict,
    *,
    variant: str = "ddl2",
    enum_to_bool: bool = True,
    enum_true: Sequence[str] = ("yes", "y", "true"),
    enum_false: Sequence[str] = ("no", "n", "false"),
    esd_col_suffix: str = "_esd_digits",
    dtype_float: pl.DataType = pl.Float64,
    dtype_int: pl.DataType = pl.Int64,
    cast_strict: bool = True,
    bool_true: Sequence[str] = ("YES",),
    bool_false: Sequence[str] = ("NO",),
    bool_strip: bool = True,
    bool_case_insensitive: bool = True,
    datetime_output: Literal["auto", "date", "datetime"] = "auto",
    datetime_time_zone: str | None = None,
) -> CIFFileValidator:
    """Create a CIF file validator from a CIF dictionary.

    Parameters
    ----------
    file
        CIF dictionary file.
    variant
        Dictionary definition language variant.
        Currently, only "ddl2" is supported.

    Returns
    -------
    CIFFileValidator
        CIF file validator instance.
    """
    if variant == "ddl2":
        return ddl2_validator(
            dictionary,
            enum_to_bool=enum_to_bool,
            enum_true=enum_true,
            enum_false=enum_false,
            esd_col_suffix=esd_col_suffix,
            dtype_float=dtype_float,
            dtype_int=dtype_int,
            cast_strict=cast_strict,
            bool_true=bool_true,
            bool_false=bool_false,
            bool_strip=bool_strip,
            bool_case_insensitive=bool_case_insensitive,
            datetime_output=datetime_output,
            datetime_time_zone=datetime_time_zone,
        )
    raise ValueError(f"Unsupported dictionary variant: {variant!r}")
