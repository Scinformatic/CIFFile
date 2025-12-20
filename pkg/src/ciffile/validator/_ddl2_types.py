"""DDL2 data type casting functions."""

from typing import Sequence, Literal

from operator import mul
from functools import reduce
import math
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


def _float(
    s: pl.Expr,
    *,
    esd_col_suffix: str = "_unc",
    float_dtype: pl.DataType = pl.Float64,
    int_dtype: pl.DataType = pl.Int64,
) -> tuple[pl.Expr, pl.Expr]:
    """Parse a string column containing floats and optional parenthesized uncertainty.

    The input strings are expected to be one of:
    - null
    - "." (a literal dot)
    - a floating-point number optionally followed by "(<digits>)"
      and optionally followed by an exponent part "e/E[+/-]<digits>".

    Behavior:
    - null stays null in both outputs.
    - "." becomes NaN in the float output and null in the integer output.
    - if no "(...)" uncertainty is present, the integer output is null.
    - the integer output column is named "<input_name><unc_suffix>".

    Parameters
    -----------
    s
        Polars expression that evaluates to a UTF8 (string) column.
        Typically this is `pl.col("...")`.
        The expression must have a resolvable output name (i.e. be a named column),
        because the uncertainty column name is derived from it.
    esd_col_suffix
        Suffix appended to the input column name for the uncertainty integer column.
    float_dtype
        Float dtype for the parsed float column.
    int_dtype
        Integer dtype for the parsed uncertainty column.

    Returns
    --------
    tuple[pl.Expr, pl.Expr]
        (float_expr, uncertainty_int_expr)

    Raises
    -------
    ValueError
        If `s` does not have a resolvable output name.

    Notes
    ------
    This uses regex-based extraction and removal of the "(digits)" component:
    - float output: remove "(digits)" then cast to float
    - uncertainty output: extract digits inside parentheses then cast to int
    """
    name = s.meta.output_name()
    if not name:
        raise ValueError(
            "Input expression must have a resolvable output name (e.g. pl.col('x'))."
        )
    values, esd = _float_core(s=s, float_dtype=float_dtype, int_dtype=int_dtype)
    return values.alias(name), esd.alias(f"{name}{esd_col_suffix}")


def _float_core(
    s: pl.Expr,
    *,
    float_dtype: pl.DataType = pl.Float64,
    int_dtype: pl.DataType = pl.Int64,
    cast_strict: bool = True,
) -> tuple[pl.Expr, pl.Expr]:
    """Parse a string column containing floats and optional parenthesized uncertainty.

    Parse a string expression to (float_value_expr, uncertainty_int_expr) without aliasing.

    This is the element-wise building block:
    - null -> (null, null)
    - "."  -> (NaN, null)
    - otherwise:
        float: remove "(digits)" then cast
        int: extract digits in "(digits)" then cast (null if missing)
    """
    # Matches "(123)" and captures "123"
    unc_re = r"\(([0-9]+)\)"

    # Float string = original with the "(digits)" part removed.
    float_str = s.str.replace_all(unc_re, "")
    float_val = (
        pl.when(s.is_null())
        .then(pl.lit(None, dtype=float_dtype))
        .when(s == pl.lit("."))
        .then(pl.lit(math.nan, dtype=float_dtype))
        .otherwise(float_str.cast(float_dtype, strict=cast_strict))
    )

    unc_val = (
        pl.when(s.is_null() | (s == pl.lit(".")))
        .then(pl.lit(None, dtype=int_dtype))
        .otherwise(s.str.extract(unc_re, group_index=1).cast(int_dtype, strict=cast_strict))
    )

    return float_val, unc_val


def _float_range(
    s: pl.Expr,
    *,
    esd_col_suffix: str = "_esd",
    float_dtype: pl.DataType = pl.Float64,
    int_dtype: pl.DataType = pl.Int64,
) -> tuple[pl.Expr, pl.Expr]:
    """Parse a string column into two fixed-size (2) Arrays: floats and uncertainties.

    Input grammar (strings):
    - null
    - "." (literal dot)
    - "<num>" or "<num>-<num>"

    <num> format:
    - optional leading "-" sign
    - mantissa: digits with optional trailing '.', OR optional leading digits + '.' + digits
      (e.g. "1", "1.", "1.23", ".5")
    - optional "(digits)" uncertainty (e.g. "1.23(45)")
    - optional exponent "e/E[+/-]digits" (e.g. "1e-3", "1.2(3)E+5")

    Outputs:
    - Float array (width 2):
        null -> null
        "."  -> [NaN, NaN]
        "<a>" -> [a, a]
        "<a>-<b>" -> [a, b]
      Uncertainty "(...)" is ignored for float parsing.
    - Integer uncertainty array (width 2), named "<col><esd_col_suffix>":
        null -> null
        "."  -> [null, null]
        numbers without "(...)" -> null at that position
        second missing -> duplicate first (including its uncertainty presence/absence)

    Parameters
    -----------
    s
        Polars expression evaluating to a UTF8 column, typically `pl.col("x")`.
        Must have a resolvable output name, because the uncertainty column name is derived.
    esd_col_suffix
        Suffix appended to the input column name for the uncertainty array column.
    float_dtype
        Float dtype for the float array elements.
    int_dtype
        Integer dtype for the uncertainty array elements.

    Returns
    -------
    tuple[pl.Expr, pl.Expr]
        (float_array_expr, uncertainty_array_expr)

    Raises
    ------
    ValueError
        If `s` does not have a resolvable output name.
    """
    name = s.meta.output_name()
    if not name:
        raise ValueError(
            "Input expression must have a resolvable output name (e.g. pl.col('x'))."
        )

    # Extract uncertainty digits inside "(...)" (capture group 1).
    unc_digits_re = r"\(([0-9]+)\)"
    # Remove "(digits)" for float casting.
    unc_remove_re = r"\([0-9]+\)"

    # Numeric token (non-capturing only): mantissa + optional uncertainty + optional exponent
    num = r"(?:[0-9]+\.?|[0-9]*\.[0-9]+)(?:\([0-9]+\))?(?:[eE][+-]?[0-9]+)?"

    # Capture only what we need:
    # 1: first token (may have leading '-')
    # 2: optional second dash after separator '-' ("" or "-") => sign for second number
    # 3: second token (no leading sign)
    full_re = rf"^(-?{num})(?:-(-?)({num}))?$"

    first_tok = s.str.extract(full_re, group_index=1)
    second_dash = s.str.extract(full_re, group_index=2)
    second_tok = s.str.extract(full_re, group_index=3)

    # ---- floats ----
    first_float = (
        first_tok.str.replace_all(unc_remove_re, "")
        .cast(float_dtype, strict=False)
    )
    second_float_unsigned = (
        second_tok.str.replace_all(unc_remove_re, "")
        .cast(float_dtype, strict=False)
    )
    second_float = (
        pl.when(second_tok.is_null())
        .then(first_float)
        .otherwise(
            pl.when(second_dash == pl.lit("-"))
            .then(-second_float_unsigned)
            .otherwise(second_float_unsigned)
        )
    )

    float_arr_dtype = pl.Array(float_dtype, 2)
    float_arr = (
        pl.when(s.is_null())
        .then(pl.lit(None, dtype=float_arr_dtype))
        .when(s == pl.lit("."))
        .then(pl.lit([math.nan, math.nan]).cast(float_arr_dtype))
        .otherwise(pl.concat_list([first_float, second_float]).list.to_array(2))
        .alias(name)
    )

    # ---- uncertainties (ints) ----
    first_unc = first_tok.str.extract(unc_digits_re, group_index=1).cast(
        int_dtype, strict=False
    )
    second_unc = second_tok.str.extract(unc_digits_re, group_index=1).cast(
        int_dtype, strict=False
    )

    # If second number is missing, duplicate the first uncertainty as well.
    second_unc_or_dup = pl.when(second_tok.is_null()).then(first_unc).otherwise(second_unc)

    int_arr_dtype = pl.Array(int_dtype, 2)
    int_arr = (
        pl.when(s.is_null())
        .then(pl.lit(None, dtype=int_arr_dtype))
        .when(s == pl.lit("."))
        .then(pl.lit([None, None], dtype=int_arr_dtype))
        .otherwise(pl.concat_list([first_unc, second_unc_or_dup]).list.to_array(2))
        .alias(f"{name}{esd_col_suffix}")
    )

    return float_arr, int_arr


def _int(
    expr: pl.Expr,
    *,
    dtype: pl.DataType = pl.Int64,
    strict: bool = True,
) -> pl.Expr:
    """Cast a Polars expression to an integer type.

    Preserves existing nulls and converts literal "." values to null
    before casting.
    Casts the given Polars expression to an integer dtype.
    By default, the cast is strict and will raise if values
    cannot be safely converted.

    Parameters
    ----------
    expr
        Polars expression to cast.
    dtype
        Target integer data type (e.g. pl.Int8, pl.Int32, pl.Int64).
    strict
        Whether to perform a strict cast.
        If True, invalid casts raise an error.
        If False, invalid values become null.

    Returns
    -------
    Polars expression with integer dtype.

    Raises
    ------
    ValueError
        If `dtype` is not an integer Polars dtype.

    Notes
    -----
    - Floating-point values are truncated toward zero.
    - Boolean expressions cast to 0/1.
    - String expressions require `strict=False` unless
      all values are valid integer literals.
    """
    if not isinstance(dtype, pl.DataType) or not dtype.is_integer():
        raise ValueError(f"dtype must be an integer Polars dtype, got {dtype!r}")

    return (
        pl.when(expr.is_null())
        .then(pl.lit(None))
        .when(expr == ".")
        .then(pl.lit(None))
        .otherwise(expr.cast(dtype, strict=strict))
    )


def _int_range(
    expr: pl.Expr,
    *,
    element_dtype: pl.DataType = pl.Int64,
    cast_strict: bool = True,
) -> pl.Expr:
    """Parse an integer range string into a fixed-size list of length 2.

    Expected input format:
        `[+-]?[0-9]+-[+-]?[0-9]+`

    Transformation rules:
    - null -> null
    - "." -> [null, null]
    - valid range -> [start, end]
    - anything else -> null

    Parameters
    ----------
    expr
        Polars expression referring to the input string column.
    element_dtype
        Integer dtype to cast the two endpoints to.
    cast_strict
        Passed to `cast(strict=...)`. If False, failed casts become null.

    Returns
    -------
    pl.Expr
        A Polars expression producing a nullable `list[element_dtype]`
        where non-null values always have exactly two elements.
    """
    # Extract both endpoints only if the *entire* string matches
    start_s = expr.str.extract(r"^([+-]?\d+)-([+-]?\d+)$", group_index=1)
    end_s = expr.str.extract(r"^([+-]?\d+)-([+-]?\d+)$", group_index=2)

    start_n = start_s.cast(element_dtype, strict=cast_strict)
    end_n = end_s.cast(element_dtype, strict=cast_strict)

    out_dtype = pl.Array(element_dtype, 2)

    range_pair = (
        pl.when(start_n.is_null() | end_n.is_null())
        .then(None)
        .otherwise(pl.concat_list([start_n, end_n]).cast(out_dtype, strict=cast_strict))
    )

    return (
        pl.when(expr.is_null())
        .then(None)
        .when(expr == ".")
        .then(pl.lit([None, None], dtype=out_dtype))
        .otherwise(range_pair)
    )


def _delimited_list(
    expr: pl.Expr,
    *,
    delimiter: str = ",",
    strip_elements: bool = True,
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


def _spaced_list(
    expr: pl.Expr,
    *,
    element_dtype: pl.DataType | None = None,
    cast_strict: bool = True,
) -> pl.Expr:
    """Parse a whitespace-separated string column into a list column.

    The input column is assumed to contain only:
    - null
    - the literal string "."
    - a non-empty list of tokens (whitespace-separated)

    Transformation rules:
    - null stays null
    - "." becomes an empty list
    - other strings are split into list elements,
      i.e., tokenizes by extracting runs of non-whitespace characters (`\\S+`),
      which is equivalent to splitting on one or more whitespace characters.
    - optionally casts each element to another dtype

    Parameters
    ----------
    expr
        Polars expression referring to the input string column.
    element_dtype
        Optional dtype to cast list elements to.
    cast_strict
        Passed to `cast(strict=...)` when `element_dtype` is provided.

    Returns
    -------
    pl.Expr
        A Polars expression producing a nullable list column.
    """
    split_expr = expr.str.extract_all(r"\S+")
    inner_dtype: pl.DataType = pl.Utf8 if element_dtype is None else element_dtype
    if element_dtype is not None:
        split_expr = split_expr.list.eval(
            pl.element().cast(element_dtype, strict=cast_strict)
        )
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
  "float": _float,
  "float-range": _float_range,
  "id_list": _delimited_list,
  "id_list_spc": _spaced_list,
  "int": _int,
  "int-range": _int_range,
  "int_list": partial(_delimited_list, element_dtype=pl.Int64),
  "symmetry_operation": _delimited_list,
  "ucode-alphanum-csv": _delimited_list,
  "yyyy-mm-dd": _partial_datetime,
  "yyyy-mm-dd:hh:mm": _partial_datetime,
  "yyyy-mm-dd:hh:mm-flex": _partial_datetime,
}