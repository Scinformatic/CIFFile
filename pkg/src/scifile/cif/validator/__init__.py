"""CIF file validator."""

import polars as pl

from ._validator import CIFFileValidator
from ._exception import CIFValidationError


def validate(df: pl.DataFrame) -> list[CIFValidationError]:
    """Validate a CIF file represented as a DataFrame.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame representation of the CIF file to be validated.

    Returns
    -------
    list[CIFValidationError]
        A list of validation errors encountered during validation.
    """
    return CIFFileValidator(df=df).validate()
