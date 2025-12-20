"""DDL2 data type validation and casting functions."""

import re

import polars as pl


_REGEX_STR = {
    '3x4_matrices': '(((([ \\t]*-?(([0-9]+)[.]?|([0-9]*[.][0-9]+))([(][0-9]+[)])?([eE][+-]?[0-9]+)? +){3})?(-?(([0-9]+)[.]?|([0-9]*[.][0-9]+))([(][0-9]+[)])?([eE][+-]?[0-9]+)? *\\n([\\t ]*\\n)*)){3})*((([ \\t]*-?(([0-9]+)[.]?|([0-9]*[.][0-9]+))([(][0-9]+[)])?([eE][+-]?[0-9]+)? +){3})?(-?(([0-9]+)[.]?|([0-9]*[.][0-9]+))([(][0-9]+[)])?([eE][+-]?[0-9]+)? *\\n([\\t ]*\\n)*)){2}((([ \\t]*-?(([0-9]+)[.]?|([0-9]*[.][0-9]+))([(][0-9]+[)])?([eE][+-]?[0-9]+)? +){3})(-?(([0-9]+)[.]?|([0-9]*[.][0-9]+))([(][0-9]+[)])?([eE][+-]?[0-9]+)? *\\n?([\\t ]*\\n)*))[ \\t]*',
    '3x4_matrix': '((([ \\t]*-?(([0-9]+)[.]?|([0-9]*[.][0-9]+))([(][0-9]+[)])?([eE][+-]?[0-9]+)? +){3})?(-?(([0-9]+)[.]?|([0-9]*[.][0-9]+))([(][0-9]+[)])?([eE][+-]?[0-9]+)? *\\n([\\t ]*\\n)*)){2}((([ \\t]*-?(([0-9]+)[.]?|([0-9]*[.][0-9]+))([(][0-9]+[)])?([eE][+-]?[0-9]+)? +){3})(-?(([0-9]+)[.]?|([0-9]*[.][0-9]+))([(][0-9]+[)])?([eE][+-]?[0-9]+)? *\\n?([\\t ]*\\n)*))[ \\t]*',
 }
_REGEX = {k: re.compile(v) for k, v in _REGEX_STR.items()}


def _3x4_matrices(col: str | pl.Expr) -> tuple[pl.Expr, pl.Expr]:
    """Validate and cast '3x4_matrices' dtype.

    Parameters
    ----------
    col
        Column name or Polars expression
        yielding string values representing
        multiple 3x4 matrices.

    Returns
    -------
    value_matrices
        Polars expression yielding matrices values
        as `pl.List(pl.Array(pl.Float64, (3, 4)))` dtype.
        - Null values remain null.
        - Inapplicable values (".") are cast to
          arrays of 3x4 matrices of NaNs.
    esd_matrices
        Polars expression yielding value uncertainties (standard deviations)
        as `pl.Array(pl.Array(pl.Float64, (3, 4)))` dtype.
        - If the input value is null, the uncertainty is also null.
        - If the input value is inapplicable ("."),
          the uncertainty is an array of 3x4 matrices of NaNs.
        - For any other input value, the uncertainty
          is an array of 3x4 matrices where each element is the
          uncertainty extracted from the input value.
          For any matrix element where no uncertainty
          is specified in the input value,
          the uncertainty is NaN.

    Raises
    ------
    ValueError
        If any non-null, non-inapplicable input value
        does not conform to the multiple 3x4 matrices format.
    """
    ...


def _3x4_matrix(col: str | pl.Expr) -> tuple[pl.Expr, pl.Expr]:
    """Validate and cast '3x4_matrix' dtype.

    Parameters
    ----------
    col
        Column name or Polars expression
        yielding string values representing 3x4 matrices.

    Returns
    -------
    value_matrix
        Polars expression yielding matrix values
        as `pl.Array(pl.Float64, (3, 4))` dtype.
        - Null values remain null.
        - Inapplicable values (".") are cast to
          3x4 matrices of NaNs.
    esd_matrix
        Polars expression yielding value uncertainties (standard deviations)
        as `pl.Array(pl.Float64, (3, 4))` dtype.
        - If the input value is null, the uncertainty is also null.
        - If the input value is inapplicable ("."),
          the uncertainty is a 3x4 matrix of NaNs.
        - For any other input value, the uncertainty
          is a 3x4 matrix where each element is the
          uncertainty extracted from the input value.
          For any matrix element where no uncertainty
          is specified in the input value,
          the uncertainty is NaN.

    Raises
    ------
    ValueError
        If any non-null, non-inapplicable input value
        does not conform to the 3x4 matrix format.
    """
    ...
