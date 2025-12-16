"""CIF data category writer."""

from __future__ import annotations

from typing import Literal, Sequence, Callable

import polars as pl


def write(
    table: pl.DataFrame,
    writer: Callable[[str], None],
    *,
    # String casting parameters
    bool_true: str = "YES",
    bool_false: str = "NO",
    null_str: Literal[".", "?"] = "?",
    null_float: Literal[".", "?"] = "?",
    null_int: Literal[".", "?"] = "?",
    null_bool: Literal[".", "?"] = "?",
    empty_str: Literal[".", "?"] = ".",
    nan_float: Literal[".", "?"] = ".",
    # Styling parameters
    always_table: bool = False,
    list_style: Literal["horizontal", "tabular", "vertical"] = "tabular",
    table_style: Literal["horizontal", "tabular-horizontal", "tabular-vertical", "vertical"] = "tabular-horizontal",
    space_items: int = 2,
    min_space_columns: int = 2,
    indent: int = 0,
    indent_inner: int = 0,
    delimiter_preference: Sequence[Literal["single", "double", "semicolon"]] = ("single", "double", "semicolon"),
    # Non-simple value rules
    special_start_chars: str = r"""^[_#\$'"\[\]]""",
    reserved_prefixes: Sequence[str] = ("data_", "save_"),
    reserved_words: Sequence[str] = ("loop_", "stop_", "global_"),
) -> None:
    """Write CIF data category in CIF syntax.

    Parameters
    ----------
    table
        Data category table as a Polars DataFrame.
        It can only contain boolean, numeric, and string columns
        (all other dtypes must be converted beforehand).
        Each column represents a data item (tag),
        and each row represents a data record.
    writer
        A callable that takes a string and writes it to the desired output.
        This could be a file write method or any other string-consuming function.
        For example, you can create a list and pass its `append` method
        to collect the output chunks into the list.
        The whole CIF content can then be obtained by joining the list elements,
        i.e., `''.join(output_list)`.
    bool_true
        Symbol to use for boolean `True` values.
    bool_false
        Symbol to use for boolean `False` values.
    null_str
        Symbol to use for null values in string columns.
    null_float
        Symbol to use for null values in floating-point columns.
    null_int
        Symbol to use for null values in integer columns.
    null_bool
        Symbol to use for null values in boolean columns.
    empty_str
        Symbol to use for empty strings in string columns.
    nan_float
        Symbol to use for NaN values in floating-point columns.
    always_table
        Whether to write the data category in table format
        even if it is a list (i.e., contains only a single row).
        When `False` (default),
        single-row categories are written as lists.
    list_style
        Style to use when writing a list (single-row category).
        Options:
        - "horizontal": All data items on a single line, separated by spaces:
          ```
          _name1 value1 _long_name2 value2 _name3 value3 ...
          ```
        - "tabular": Each data item on its own line, aligned in a table:
          ```
          _name1       value1
          _long_name2  value2
          _name3       value3
          ...
          ```
        - "vertical": Each token on its own line:
          ```
          _name1
          value1
          _long_name2
          value2
          _name3
          value3
          ...
          ```
    table_style
        Style to use when writing a table (multi-row category).
        Options:
        - "horizontal": All tokens on a single line, separated by spaces:
          ```
          loop_ _name1 _long_name2 _name3 value1_1 value2_1 value3_1 value1_2 value2_2 value3_2 ...
          ```
        - "tabular-horizontal": Each row (including headers) on its own line,
          aligned in a table:
          ```
          loop_
          _name1    _long_name2  _name3
          value1_1  value2_1     value3_1
          value1_2  value2_2     value3_2
          ...
          ```
        - "tabular-vertical": Vertical header with each row on its own line,
          aligned in a table:
          ```
          loop_
          _name1
          _long_name2
          _name3
          value1_1  value2_1  value3_1
          value1_2  value2_2  value3_2
          ...
          ```
        - "vertical": Each token on its own line:
          ```
          loop_
          _name1
          _long_name2
          _name3
          value1_1
          value2_1
          value3_1
          value1_2
          value2_2
          value3_2
          ...
          ```
    space_items
        Number of spaces to use
        between name-value pairs in horizontal lists:
        ```
        _name1 value1<space_items>_long_name2 value2 ...
        ```
    min_space_columns
        Minimum number of spaces to use
        between columns in tabular formats:
        ```
        _name1  <min_space_columns>_long_name2<min_space_columns>_name3
        value1_1<min_space_columns>value2_1   <min_space_columns>value3_1
        ...
        ```
    indent
        Number of spaces to indent each line
        of the overall data category output:
        ```
        <indent>loop_
        <indent>_name1 _name2 ...
        <indent>value1_1 value2_1 ...
        ```
    indent_inner
        Number of spaces to indent each line
        inside loop constructs (table body):
        ```
        loop_
        <indent_inner>_name1 _name2 ...
        <indent_inner>value1_1 value2_1 ...
        ```
    delimiter_preference
        Order of preference for string delimiters/quotations,
        from most to least preferred.
    special_start_chars
        Regex character class (string) of characters that cannot start an unquoted "simple" value.
    reserved_prefixes
        Sequence of reserved prefixes that cannot start an unquoted "simple" value.
    reserved_words
        Sequence of reserved words that cannot be used as unquoted "simple" values.

    Returns
    -------
    None
        Uses the provided `writer` callable to output the CIF data category.
    """
    cat = _normalize_data_values(
        table,
        bool_true=bool_true,
        bool_false=bool_false,
        null_str=null_str,
        null_float=null_float,
        null_int=null_int,
        null_bool=null_bool,
        empty_str=empty_str,
        nan_float=nan_float,
        delimiter=delimiter_preference,
        special_start_chars=special_start_chars,
        reserved_prefixes=reserved_prefixes,
        reserved_words=reserved_words,
    )




def _normalize_data_values(
    df: pl.DataFrame,
    *,
    bool_true: str = "YES",
    bool_false: str = "NO",
    null_str: Literal[".", "?"] = "?",
    null_float: Literal[".", "?"] = "?",
    null_int: Literal[".", "?"] = "?",
    null_bool: Literal[".", "?"] = "?",
    empty_str: Literal[".", "?"] = ".",
    nan_float: Literal[".", "?"] = ".",
    delimiter: Sequence[Literal["single", "double", "semicolon"]] = ("single", "double", "semicolon"),
    special_start_chars: str = r"""^[_#\$'"\[\]]""",
    reserved_prefixes: Sequence[str] = ("data_", "save_"),
    reserved_words: Sequence[str] = ("loop_", "stop_", "global_"),
) -> pl.DataFrame:
    """Normalize data values in the CIF DataFrame.

    This function normalizes all values in the given DataFrame
    into UTF-8 strings that can be directly written into a CIF/mmCIF file.
    It uses Polars expressions for fast, vectorized execution.

    The process includes:
    - Casting all columns to Utf8.
    - Mapping booleans to `bool_true` / `bool_false`.
    - Replacing nulls with CIF missing/inapplicable symbols ('.' or '?').
    - Replacing NaN in float columns.
    - Replacing empty strings in string columns.
    - Quoting/delimiting string values only when needed:
      - leave "simple" values unquoted
      - otherwise wrap in preferred quotes if safe, else fall back to text-field
        style using semicolon delimiters.

    Parameters
    ----------
    df
        CIF DataFrame with data values to normalize.
        It can only contain boolean, numeric, and string columns.
    bool_true
        Symbol to use for boolean `True` values.
    bool_false
        Symbol to use for boolean `False` values.
    null_str
        Symbol to use for null values in string columns.
    null_float
        Symbol to use for null values in floating-point columns.
    null_int
        Symbol to use for null values in integer columns.
    null_bool
        Symbol to use for null values in boolean columns.
    empty_str
        Symbol to use for empty strings in string columns.
    nan_float
        Symbol to use for NaN values in floating-point columns.
    delimiter
        Order of preference for string delimiters/quotations,
        from most to least preferred.
    special_start_chars
        Regex character class (string) of characters that cannot start an unquoted "simple" value.
    reserved_prefixes
        Sequence of reserved prefixes that cannot start an unquoted "simple" value.
    reserved_words
        Sequence of reserved words that cannot be used as unquoted "simple" values.

    Returns
    -------
    normalized_df
        CIF DataFrame with normalized data values.

    Raises
    ------
    TypeError
        If the input DataFrame contains unsupported dtypes.
    ValueError
        If any multiline string contains a line beginning with ';',
        which cannot be represented exactly as a CIF 1.1 text field.
    """

    expressions: list[pl.Expr] = []
    # Collect per-string-column “unrepresentable” boolean expressions here, but build them
    # in the SAME schema loop (no second schema loop).
    unrepresentable_checks: list[pl.Expr] = []

    for name, dtype in df.schema.items():
        col = pl.col(name)

        if dtype == pl.Boolean:
            expressions.append(
                pl.when(col.is_null())
                .then(pl.lit(null_bool))
                .otherwise(
                    pl.when(col)
                    .then(pl.lit(bool_true))
                    .otherwise(pl.lit(bool_false))
                )
                .alias(name)
            )

        elif dtype.is_integer():
            expressions.append(
                pl.when(col.is_null())
                .then(pl.lit(null_int))
                .otherwise(col.cast(pl.Utf8))
                .alias(name)
            )

        elif dtype.is_float():
            expressions.append(
                pl.when(col.is_null())
                .then(pl.lit(null_float))
                .otherwise(
                    pl.when(col.is_nan())
                    .then(pl.lit(nan_float))
                    .otherwise(col.cast(pl.Utf8))
                )
                .alias(name)
            )

        elif dtype == pl.Utf8:
            # Build normalization expression
            expr = (
                pl.when(col.is_null())
                .then(pl.lit(null_str))
                .otherwise(
                    pl.when(col == "")
                    .then(pl.lit(empty_str))
                    .otherwise(col)
                )
            )
            final_expr, is_unrepresentable = _quote_string_col(
                expr,
                delimiter=delimiter,
                special_start_chars=special_start_chars,
                reserved_prefixes=reserved_prefixes,
                reserved_words=reserved_words,
            )
            expressions.append(final_expr.alias(name))
            unrepresentable_checks.append(is_unrepresentable.alias(f"__bad__{name}"))

        else:
            raise TypeError(
                f"Unsupported dtype for column {name!r}: {dtype}. "
                "Only Boolean, integer, float, and Utf8 columns are allowed."
            )

    # Evaluate the representability check only once.
    if unrepresentable_checks:
        bad_any = df.select(pl.any_horizontal(unrepresentable_checks).alias("_bad")).item()
        if bool(bad_any):
            raise ValueError(
                "At least one multiline string contains a line beginning with ';'. "
                "This cannot be represented exactly as a CIF 1.1 text field."
            )

    return df.with_columns(expressions)


def _quote_string_col(
    col: pl.Expr,
    *,
    delimiter: Sequence[Literal["single", "double", "semicolon"]],
    special_start_chars: str,
    reserved_prefixes: Sequence[str],
    reserved_words: Sequence[str],
) -> tuple[pl.Expr, pl.Expr]:
    """Normalize a UTF-8 column into CIF-ready tokens (unquoted, quoted, or text fields).

    The operation is fully vectorized and uses Polars string/conditional expressions.

    Steps:
    1) Determine whether delimiting is required (CIF "simple" vs "non-simple").
    2) For multiline values, force semicolon-delimited text fields.
    3) For non-simple single-line values:
       - prefer the requested quote style if safe
       - otherwise try the other quote style if safe
       - otherwise fall back to semicolon text field

    Parameters
    ----------
    col
        Polars expression for the string column (may not contain nulls).
    delimiter
        Order of preference for string delimiters/quotations,
        from most to least preferred.
    special_start_chars
        Regex character class (string) of characters that cannot start an unquoted "simple" value.
    reserved_prefixes
        Sequence of reserved prefixes that cannot start an unquoted "simple" value.
    reserved_words
        Sequence of reserved words that cannot be used as unquoted "simple" values.

    Returns
    -------
    pl.Expr
        UTF-8 expression producing CIF-ready string tokens.

    Notes
    -----
    A CIF “simple” (unquoted) character value must:
    - be single-line,
    - contain no whitespace (space/tab),
    - not start with CIF-special token starters (e.g. '_', '#', quotes),
    - not start with reserved prefixes ('data_', 'save_' case-insensitive),
    - not equal reserved words ('loop_', 'stop_', 'global_' case-insensitive).
    """
    # Determine which values need delimiting
    has_whitespace = col.str.contains(r"[ \t]")
    is_multiline = col.str.contains(r"[\r\n]")
    # Characters that cannot start an unquoted "simple" value.
    starts_special_char = col.str.contains(special_start_chars)
    # Lowercase version for prefix/word checks
    col_lowercase = col.str.to_lowercase()
    starts_reserved_prefix = pl.any_horizontal(
        [col_lowercase.str.starts_with(p) for p in reserved_prefixes]
    )
    equals_reserved_word = col_lowercase.is_in(list(reserved_words))
    need_delim = is_multiline | has_whitespace | starts_special_char | starts_reserved_prefix | equals_reserved_word

    # Determine which quoting styles are safe
    safe_single = _is_safe_for_single_quotes(col)
    safe_double = _is_safe_for_double_quotes(col)

    # CIF 1.1 text-field hard limitation check:
    #   18. A text field delimited by the <eol>; digraph
    #   may not include a semicolon at the start of a line of text as part of its value.
    is_unrepresentable = (
        is_multiline & col.str.contains(r"(?m)^;")
    )

    # choose quote preference, but only if safe; otherwise fallback to the other; else semicolon
    quoted: pl.Expr | None = None
    for d in delimiter:
        if d == "single":
            cond = safe_single
            val = pl.concat_str([pl.lit("'"), col, pl.lit("'")])
        elif d == "double":
            cond = safe_double
            val = pl.concat_str([pl.lit('"'), col, pl.lit('"')])
        elif d == "semicolon":
            # semicolon text fields do not have a "safety" predicate here;
            # representability must be validated elsewhere
            cond = pl.lit(True)
            val = _to_text_field(col)
        else:
            raise ValueError(f"Invalid delimiter value: {d!r}")
        quoted = pl.when(cond).then(val) if quoted is None else quoted.when(cond).then(val)
    if quoted is None:
        raise ValueError("No valid delimiter specified.")
    # Hard fallback (defensive): semicolon text field
    quoted = quoted.otherwise(_to_text_field(col))

    # Multiline forces text fields; otherwise only delimit if needed.
    return (
        pl.when(is_multiline)
        .then(_to_text_field(col))
        .when(need_delim)
        .then(quoted)
        .otherwise(col)
    ), is_unrepresentable


def _is_safe_for_single_quotes(s: pl.Expr) -> pl.Expr:
    """Check whether a string can be safely wrapped in single quotes in CIF 1.1.

    CIF quote-delimited strings do not use escapes.
    The delimiter quote character may appear inside the string
    only when it is NOT followed by whitespace or end-of-string.
    Example: `'a dog's life'` is legal because
    the internal `'` is followed by `s`, not whitespace.

    This expression is True iff there is no occurrence of `'` that is followed by
    whitespace or end-of-string.

    Parameters
    ----------
    s
        Polars expression yielding UTF-8 strings (may not contain nulls).

    Returns
    -------
    pl.Expr
        Boolean expression: True means single-quoting is syntactically safe.
    """
    return ~s.str.contains(r"'(?=\s|$)")


def _is_safe_for_double_quotes(s: pl.Expr) -> pl.Expr:
    """Check whether a string can be safely wrapped in double quotes in CIF 1.1.

    Same rule as for single quotes:
    the delimiter character `"` may appear inside the
    quoted string only if it is NOT followed by whitespace or end-of-string.

    Parameters
    ----------
    s
        Polars expression yielding UTF-8 strings (may not contain nulls).

    Returns
    -------
    pl.Expr
        Boolean expression: True means double-quoting is syntactically safe.
    """
    return ~s.str.contains(r'"(?=\s|$)')


def _to_text_field(s: pl.Expr) -> pl.Expr:
    """Wrap a string into a CIF 1.1 semicolon-delimited text field token.

    The produced token has the form:

        ;\\n<value>\\n;

    This is the only CIF 1.1 representation that can carry multi-line values
    (quote-delimited values must not span lines).

    IMPORTANT LIMITATION (CIF 1.1):
    The content of a text field cannot contain any line whose first character is `;`,
    because `<eol>;` terminates the field and there is no escaping mechanism.

    This function only *constructs* the token; callers must separately validate that
    the content is representable when needed.

    Parameters
    ----------
    s
        Polars expression yielding UTF-8 strings (may not contain nulls).

    Returns
    -------
    pl.Expr
        UTF-8 expression representing the semicolon-delimited field token.
    """
    delim = pl.lit("\n;")
    return pl.concat_str([delim, s, delim])

