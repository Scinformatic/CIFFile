"""CIF file validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._validator import CIFFileValidator

if TYPE_CHECKING:
    import polars as pl
    from ._exception import CIFValidationError


def validate(df: pl.DataFrame) -> list[CIFValidationError]:
    """Validate a CIF file represented as a DataFrame.

    Parameters
    ----------
    df
        DataFrame representation of the CIF file to be validated.

    Returns
    -------
    validation_errors
        List of validation errors encountered during validation.
    """
    return CIFFileValidator(df=df).validate()
