"""CIF data category writer."""

from __future__ import annotations

from typing import Literal, Sequence, Callable, overload

import polars as pl

__all__ = [
    "write",
]


@overload
def write(
    table: pl.DataFrame,
    writer: None = None,
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
) -> str: ...

@overload
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
) -> None: ...

def write(
    table: pl.DataFrame,
    writer: Callable[[str], None] | None = None,
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
) -> str | None:
    """Write a CIF data category in CIF format.

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
        If `None` (default), the function will return
        the entire CIF content as a single string.
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
    cif_content
        If `writer` is not provided (i.e., is `None`),
        the entire CIF content is returned as a single string.
        Otherwise, the provided `writer` callable
        is used to output the CIF data category,
        and `None` is returned.

    Raises
    ------
    TypeError
        If the input DataFrame contains unsupported dtypes.
    ValueError
        If any multiline string contains a line beginning with ';',
        which cannot be represented exactly as a CIF 1.1 text field.
    """
    if indent < 0 or indent_inner < 0:
        raise ValueError("indent and indent_inner must be >= 0")
    if space_items < 1:
        raise ValueError("space_items must be >= 1")
    if min_space_columns < 1:
        raise ValueError("min_space_columns must be >= 1")

    if writer is None:
        chunks: list[str] = []
        writer = chunks.append
        writer_provided = False
    else:
        writer_provided = True

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

    names = list(cat.columns)
    n_rows = cat.height
    n_cols = len(names)
    if n_cols == 0:
        return

    # Writing is inherently iterative. Convert once to Python strings.
    rows: list[list[str]] = cat.select([pl.col(c) for c in names]).rows()

    ind0 = " " * indent
    ind_in = " " * (indent + indent_inner)

    def _write_line(line: str, *, inner: bool = False) -> None:
        writer((ind_in if inner else ind0) + line + "\n")

    def _write_token(tok: str, *, inner: bool = False) -> None:
        # Text fields must have ';' in column 1. Do not indent them.
        if "\n" in tok:
            # Ensure token begins with ";\n" and ends with "\n;" (already enforced by _to_text_field).
            writer(tok)
            if not tok.endswith("\n"):
                writer("\n")
            return
        _write_line(tok, inner=inner)

    def _any_multiline_token() -> bool:
        for r in rows:
            for v in r:
                if "\n" in v:
                    return True
        return False

    # If any value is a text field token, styles that place values on the same line as tags/other
    # tokens can break CIF syntax. Force a safe vertical layout in that case.
    has_text_fields = _any_multiline_token()
    if has_text_fields:
        if n_rows == 1 and not always_table:
            if list_style == "horizontal":
                list_style = "tabular"  # or "vertical", but tabular is fine and preserves intent
        else:
            # For loop tables, any multiline token breaks row-based formats.
            table_style = "vertical"

    as_table = always_table or n_rows != 1

    if not as_table:
        row = rows[0]

        if list_style == "horizontal":
            sep_pairs = " " * space_items
            parts: list[str] = []
            for k, v in zip(names, row, strict=True):
                # multiline tokens cannot appear on same line; forced away above.
                parts.append(f"{k} {v}")
            _write_line(sep_pairs.join(parts))

        elif list_style == "tabular":
            max_name = max(len(k) for k in names)
            for k, v in zip(names, row, strict=True):
                if "\n" in v:
                    _write_line(k)
                    _write_token(v)
                else:
                    pad = " " * (max_name - len(k) + min_space_columns)
                    _write_line(f"{k}{pad}{v}")

        elif list_style == "vertical":
            for k, v in zip(names, row, strict=True):
                _write_line(k)
                _write_token(v)

        else:
            raise ValueError(f"Invalid list_style: {list_style!r}")

        return

    # ----- TABLE (loop_) -----
    _write_line("loop_")

    # Compute widths for tabular styles using written tokens (single-line only).
    col_widths: list[int] = []
    if table_style == "tabular-horizontal":
        for j in range(n_cols):
            w = len(names[j])  # headers share a row, so include header width
            for i in range(n_rows):
                w = max(w, len(rows[i][j]))
            col_widths.append(w)

    elif table_style == "tabular-vertical":
        for j in range(n_cols):
            w = 0  # headers are vertical, so do NOT include header width
            for i in range(n_rows):
                w = max(w, len(rows[i][j]))
            col_widths.append(w)

    if table_style == "horizontal":
        tokens: list[str] = []
        tokens.extend(names)
        for r in rows:
            tokens.extend(r)
        _write_line(" ".join(tokens), inner=True)

    elif table_style == "tabular-horizontal":
        hdr = [
            names[j] + (" " * (col_widths[j] - len(names[j])))
            for j in range(n_cols)
        ]
        _write_line((" " * min_space_columns).join(hdr), inner=True)
        for r in rows:
            vals = [
                r[j] + (" " * (col_widths[j] - len(r[j])))
                for j in range(n_cols)
            ]
            _write_line((" " * min_space_columns).join(vals), inner=True)

    elif table_style == "tabular-vertical":
        for k in names:
            _write_line(k, inner=True)
        for r in rows:
            vals = [
                r[j] + (" " * (col_widths[j] - len(r[j])))
                for j in range(n_cols)
            ]
            _write_line((" " * min_space_columns).join(vals), inner=True)

    elif table_style == "vertical":
        for k in names:
            _write_line(k, inner=True)
        for r in rows:
            for v in r:
                _write_token(v, inner=True)

    else:
        raise ValueError(f"Invalid table_style: {table_style!r}")

    if writer_provided:
        return None
    return "".join(chunks)


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
        bad_any = df.select(pl.any_horizontal(unrepresentable_checks).any().alias("_bad")).item()
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
    whitespace, and the string does not end with `'`.

    Parameters
    ----------
    s
        Polars expression yielding UTF-8 strings (may not contain nulls).

    Returns
    -------
    pl.Expr
        Boolean expression: True means single-quoting is syntactically safe.
    """
    followed_by_ws = s.str.contains(r"'[ \t\r\n]")
    trailing = s.str.ends_with("'")
    return ~(followed_by_ws | trailing)


def _is_safe_for_double_quotes(s: pl.Expr) -> pl.Expr:
    """Check whether a string can be safely wrapped in double quotes in CIF 1.1.

    Same rule as for single quotes:
    the delimiter character `"` may appear inside the
    quoted string only if it is NOT followed by whitespace or end-of-string.

    This expression is True iff there is no occurrence of `"` that is followed by
    whitespace, and the string does not end with `"`.

    Parameters
    ----------
    s
        Polars expression yielding UTF-8 strings (may not contain nulls).

    Returns
    -------
    pl.Expr
        Boolean expression: True means double-quoting is syntactically safe.
    """
    followed_by_ws = s.str.contains(r'"[ \t\r\n]')
    trailing = s.str.ends_with('"')
    return ~(followed_by_ws | trailing)


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
    return pl.concat_str([pl.lit(";"), s, pl.lit("\n;")])
