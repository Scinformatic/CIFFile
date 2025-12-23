"""DDL2 CIF file validator.

References
----------
- [DDL Specification](https://www.iucr.org/resources/cif/spec/ddl)
- [DDL2 Specification](https://www.iucr.org/resources/cif/ddl/ddl2)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._validator import DDL2Validator
from ._gen import DDL2Generator

if TYPE_CHECKING:
    from ciffile.structure import CIFFile, CIFBlock


validator = DDL2Validator


__all__ = [
    "DDL2Generator",
    "DDL2Validator",
    "dictionary",
    "validator",
]


def dictionary(file: CIFFile | CIFBlock) -> dict:
    """Create a CIF file validator from a CIF dictionary.

    Parameters
    ----------
    file
        CIF dictionary file.

    Returns
    -------
    CIFFileValidator
        CIF file validator instance.
    """
    return DDL2Generator(file).generate()
