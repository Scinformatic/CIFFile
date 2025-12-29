from typing import overload

import polars as pl


@overload
def normalize_whitespace(
    target: str,
    df: None,
) -> str: ...

@overload
def normalize_whitespace(
    target: list[str] | pl.Expr,
    df: None,
) -> pl.Expr: ...

@overload
def normalize_whitespace(
    target: str | list[str] | pl.Expr,
    df: pl.DataFrame,
) -> pl.DataFrame: ...

def normalize_whitespace(
    target: str | list[str] | pl.Expr,
    df: pl.DataFrame | None = None,
) -> str | pl.Expr | pl.DataFrame:
    """Normalize whitespace in a string or DataFrame column(s).

    This replaces all sequences of whitespace characters (including newlines)
    with a single space and trims leading/trailing whitespace.

    Parameters
    ----------
    target
        Normalization target.
        If `df` is `None`:
        - If a string is provided, that string is normalized and returned.
        - If a list of strings or a Polars expression is provided, a Polars expression
          for normalization is returned.
    df
        DataFrame containing the column(s) to normalize specified in `target`.
        When provided, the normalization is applied to the specified column(s)
        and the updated DataFrame is returned.
        Target can be a single column name, a list of column names,
        or a Polars expression selecting the column(s).

    Returns
    -------
    normalized
        Normalized string, Polars string normalization expression,
        or DataFrame with normalized column(s).
    """
    no_df = df is None

    # Strings are treated as literal normalization when df is None
    if no_df and isinstance(target, str):
        return " ".join(target.split())

    # Build expression over the selected columns/expr
    if not isinstance(target, pl.Expr):
        target = pl.col(target)
    expr = (
        target
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
    )
    if no_df:
        return expr
    return df.with_columns(expr)
