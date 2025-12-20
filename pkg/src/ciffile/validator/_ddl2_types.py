"""DDL2 data type casting functions."""

from typing import Sequence, Literal

import re
from functools import partial

import polars as pl



def cast(col: str | pl.Expr, dtype: str) -> list[pl.Expr]:
    return


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


def _boolean(
    expr: pl.Expr,
    *,
    true_values: Sequence[str] = ("true", "yes", "y", "1"),
    false_values: Sequence[str] = ("false", "no", "n", "0"),
    strip: bool = True,
    case_insensitive: bool = True,
) -> pl.Expr:
    """Convert a string column to boolean using explicit truthy/falsey vocabularies.

    The mapping rules are:
    - null stays null
    - values in `true_values` -> True
    - values in `false_values` -> False
    - any other value -> null

    Optionally, the comparison can be normalized by trimming whitespace and/or
    lowercasing both the input and the vocab lists.

    Parameters
    ----------
    expr
        Polars expression referring to the input column (typically `pl.Utf8`).
    true_values
        Strings that should map to True.
    false_values
        Strings that should map to False.
    strip
        If True, trims leading/trailing whitespace on the input before matching.
        (Also applies to the provided vocab lists.)
    case_insensitive
        If True, matches case-insensitively by lowercasing the input and vocab lists.

    Returns
    -------
    pl.Expr
        A Polars expression producing a nullable boolean column (`pl.Boolean`),
        with unmatched values mapped to null.

    Raises
    -------
    ValueError
        If `true_values` and `false_values` overlap after normalization.
    """
    def _norm(s: str) -> str:
        if strip:
            s = s.strip()
        if case_insensitive:
            s = s.lower()
        return s

    true_set = {_norm(s) for s in true_values}
    false_set = {_norm(s) for s in false_values}
    overlap = true_set & false_set
    if overlap:
        raise ValueError(
            "true_values and false_values overlap after normalization: "
            + ", ".join(sorted(overlap))
        )

    normalized = expr
    if strip:
        normalized = normalized.str.strip_chars()
    if case_insensitive:
        normalized = normalized.str.to_lowercase()

    return (
        pl.when(expr.is_null())
        .then(None)
        .when(normalized.is_in(list(true_set)))
        .then(pl.lit(True, dtype=pl.Boolean))
        .when(normalized.is_in(list(false_set)))
        .then(pl.lit(False, dtype=pl.Boolean))
        .otherwise(None)
    )


def _delimited_list(
    expr: pl.Expr,
    *,
    delimiter: str = ",",
    strip_elements: bool = False,
    element_dtype: pl.DataType | None = None,
    cast_strict: bool = True,
) -> pl.Expr:
    """Parse a delimited string column into a list column.

    This expression transformer assumes the input column contains only:
    - null
    - the literal string "."
    - a delimited string of non-empty elements (e.g., "a,b,c")

    Transformation rules:
    - null stays null
    - "." becomes an empty list
    - other strings are split by `delimiter` into a list
    - optionally trims each element
    - optionally casts each element to `element_dtype`

    Parameters
    ----------
    expr
        Polars expression referring to the input string column.
    delimiter
        Delimiter used to split the string into list elements.
    strip_elements
        If True, trims leading/trailing whitespace from each element after splitting.
    element_dtype
        If provided, casts each element to this dtype (e.g. `pl.Int64`, `pl.Float64`,
        `pl.Date`, etc.). If None, elements remain strings (`pl.Utf8`).
    cast_strict
        Passed through to `cast(strict=...)`. If True, invalid casts error.
        If False, invalid casts become null.

    Returns
    -------
    pl.Expr
        A Polars expression producing `list[element_dtype]` (or `list[str]` if
        `element_dtype is None`), with null preserved and "." mapped to an empty list.

    Raises
    -------
    ValueError
        If `delimiter` is empty.
    """
    if delimiter == "":
        raise ValueError("`delimiter` must be a non-empty string.")

    inner_dtype: pl.DataType = pl.Utf8 if element_dtype is None else element_dtype

    split_expr = expr.str.split(delimiter)

    if strip_elements:
        split_expr = split_expr.list.eval(pl.element().str.strip_chars())

    if element_dtype is not None:
        split_expr = split_expr.list.eval(pl.element().cast(element_dtype, strict=cast_strict))

    return (
        pl.when(expr.is_null())
        .then(None)
        .when(expr == ".")
        .then(pl.lit([], dtype=pl.List(inner_dtype)))
        .otherwise(split_expr)
    )


def _partial_datetime(
    expr: pl.Expr,
    *,
    output: Literal["auto", "date", "datetime"] = "auto",
    time_zone: str | None = None,
) -> pl.Expr:
    """Parse partial date/datetime strings into Polars Date/Datetime.

    Parameters
    ----------
    expr
        Polars expression evaluating to a string column.
        Accepted input is a *partial* form of `yyyy-mm-dd:hh:mm`,
        where only the year is required.
        The actual accepted shape is:

        ```
        y{2,3}[-m{1,2}[-d{1,2}]][:h{1,2}[:min{1,2}]]
        ```

        Year normalization rules are:
        - If 2 digits: prefix with "20" (e.g. "22" -> "2022").
        - If 3 digits:
          - if first digit is "0": prefix with "2" (e.g. "022" -> "2022")
          - otherwise: prefix with "1" (e.g. "998" -> "1998")

        Other missing components are defaulted as follows:
        - month/day default to "01"
        - hour/minute default to "00"

        A literal "." is treated as null.
    output
        Desired output type:
        - `"date"` returns a `pl.Date`.
        - `"datetime"` returns a `pl.Datetime` (missing time becomes 00:00).
        - `"auto"` returns a `pl.Datetime` as the common supertype:
            rows without an explicit time become midnight.
    time_zone
        Optional time zone for `pl.Datetime` parsing/casting.

    Returns
    -------
    pl.Expr
        Expression converting the input to `pl.Date` or `pl.Datetime`.
    """
    # Regex matching the full allowed grammar.
    # Capture groups:
    #   1 = year (2–3 digits)
    #   2 = month (1–2 digits, optional)
    #   3 = day (1–2 digits, optional)
    #   4 = hour (1–2 digits, optional)
    #   5 = minute (1–2 digits, optional)
    pattern = (
        r"^(\d{2,3})"
        r"(?:-(\d{1,2})(?:-(\d{1,2}))?)?"
        r"(?::(\d{1,2})(?::(\d{1,2}))?)?$"
    )

    # Normalize input:
    # - cast to string
    # - trim whitespace
    # - turn "." into null
    s = (
        expr.cast(pl.Utf8)
        .str.strip_chars()
        .replace(".", None)
    )

    # Extract raw components
    y_raw = s.str.extract(pattern, 1)
    m_raw = s.str.extract(pattern, 2)
    d_raw = s.str.extract(pattern, 3)
    h_raw = s.str.extract(pattern, 4)
    min_raw = s.str.extract(pattern, 5)

    # Length of the year token (2 or 3)
    y_len = y_raw.str.len_chars()

    # ---- Year normalization ----

    # Case: 2-digit year -> "20" + yy
    year_2 = pl.concat_str([pl.lit("20"), y_raw])

    # Case: 3-digit year
    # First digit determines century
    y_first = y_raw.str.slice(0, 1)

    century_prefix = (
        pl.when(y_first == pl.lit("0"))
        .then(pl.lit("2"))   # 0xx -> 20xx
        .otherwise(pl.lit("1"))  # xxx -> 19xx
    )

    year_3 = pl.concat_str([century_prefix, y_raw])

    # Select correct year normalization
    year4 = (
        pl.when(y_len == 2).then(year_2)
        .when(y_len == 3).then(year_3)
        .otherwise(None)
    )

    def zero_pad(value: pl.Expr, default: str) -> pl.Expr:
        """Zero-pad to two digits and apply default if missing."""
        return value.fill_null(pl.lit(default)).str.zfill(2)

    # Apply defaults and padding
    mm = zero_pad(m_raw, "01")
    dd = zero_pad(d_raw, "01")
    hh = zero_pad(h_raw, "00")
    mi = zero_pad(min_raw, "00")

    # Build canonical strings for parsing
    date_str = pl.concat_str(
        [
            year4,
            pl.lit("-"),
            mm,
            pl.lit("-"),
            dd,
        ]
    )
    datetime_str = pl.concat_str(
        [
            date_str,
            pl.lit(" "),
            hh,
            pl.lit(":"),
            mi,
        ]
    )

    # Parse into Polars dtypes
    date_expr = date_str.str.strptime(
        pl.Date,
        format="%Y-%m-%d",
        strict=True,
    )

    dt_dtype = (
        pl.Datetime(time_zone=time_zone)
        if time_zone is not None
        else pl.Datetime
    )

    datetime_expr = datetime_str.str.strptime(
        dt_dtype,
        format="%Y-%m-%d %H:%M",
        strict=False,
    )

    # Detect whether a row explicitly contained time information
    has_time = h_raw.is_not_null() | min_raw.is_not_null()

    # Output selection
    if output == "date":
        return date_expr

    if output == "datetime":
        return datetime_expr

    # output="auto":
    #   return a Datetime column;
    #   date-only rows become midnight
    return (
        pl.when(has_time)
        .then(datetime_expr)
        .otherwise(date_expr.cast(dt_dtype))
    )


_CODE_TO_CAST_FUNC = {
  "boolean": _boolean,
  "date_dep": _partial_datetime,
  "entity_id_list": _delimited_list,
  "id_list": _delimited_list,
  "int_list": partial(_delimited_list, element_dtype=pl.Int64),
  "ucode-alphanum-csv": _delimited_list,
  "yyyy-mm-dd": _partial_datetime,
  "yyyy-mm-dd:hh:mm": _partial_datetime,
  "yyyy-mm-dd:hh:mm-flex": _partial_datetime,
}