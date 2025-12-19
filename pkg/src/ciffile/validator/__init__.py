"""CIF file validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._ddl2 import DDL2Validator
from ._ddl2_gen import DDL2Generator

if TYPE_CHECKING:
    from ciffile.structure import CIFFile, CIFBlock


__all__ = [
    "DDL2Validator",
    "dictionary",
]


def dictionary(
    file: CIFFile | CIFBlock | dict,
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
        generator = DDL2Generator
        validator = DDL2Validator
    else:
        raise ValueError(f"Unsupported dictionary variant: {variant!r}")
    if isinstance(file, dict):
        dict_data = file
    else:
        dict_data = generator(file).generate()
    return validator(dict_data)
