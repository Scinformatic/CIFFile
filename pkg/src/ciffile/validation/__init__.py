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
        return ddl2_validator(dictionary)
    raise ValueError(f"Unsupported dictionary variant: {variant!r}")
