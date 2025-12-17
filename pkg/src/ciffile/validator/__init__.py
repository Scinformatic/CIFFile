"""CIF file validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._ddl2 import DDL2Validator

if TYPE_CHECKING:
    from ciffile.structure import CIFFile


__all__ = [
    "DDL2Validator",
    "dictionary",
]


def dictionary(
    file: CIFFile,
    *,
    variant: str = "ddl2",
) -> DDL2Validator:
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
        return DDL2Validator(file)
    raise ValueError(f"Unsupported dictionary variant: {variant!r}")
