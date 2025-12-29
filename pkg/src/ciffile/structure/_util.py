from typing import Literal, Any
from collections.abc import Hashable
import warnings

import polars as pl

from ciffile.typing import DataFrameLike


def dataframe_to_dict(
    df: pl.DataFrame,
    ids: str | list[str],
    flat: bool = False,
    single_col: Literal["value", "dict"] = "value",
    single_row: Literal["value", "list"] = "value",
    multi_row: Literal["list", "first", "last"] = "list",
    multi_row_warn: bool = False,
    df_name: str | None = None,
) -> dict[Any, Any]:
    """Convert DataFrame to dictionary.

    Parameters
    ----------
    df
        DataFrame to convert.
    ids
        Column name(s) to use as dictionary keys.
        ID values must be hashable to be used as dictionary keys.
    flat
        How to structure the output dictionary's ID dimensions
        when multiple IDs are provided:
        - If `True`, the output dictionary will only have one ID dimension,
          with keys corresponding to ID tuples.
        - If `False`, the output dictionary will be nested,
          with the first ID values as first-dimension keys,
          the second ID values as second-dimension keys, and so on.

        When only one ID is provided, this parameter has no effect
        and the output will always have a single ID dimension
        with keys corresponding to the ID values.
    single_col
        How to structure the output dictionary's data dimension
        (i.e., the value of the inner-most ID dictionary)
        when there is only one data (non-ID) column in the DataFrame:
        - If "value", the output dictionary's data dimension will be
          the column value directly.
        - If "dict", the output dictionary's data dimension will be dictionaries
          with the data column name as key and the column value as value.

        When there are multiple data columns, this parameter has no effect
        and the output will always have dictionaries as the data dimension.
    single_row
        How to handle ID groups that correspond to a single row:
        - If "value", data values are returned directly.
        - If "list", data values are returned as single-item lists.
    multi_row
        How to handle ID groups that correspond to multiple rows:
        - If "list", data values are returned as lists.
        - If "first", only the first row's data values are returned.
        - If "last", only the last row's data values are returned.
    multi_row_warn
        If `True`, issue a warning when dropping rows,
        i.e., when ID groups correspond to multiple rows
        and `multi_row` is set to "first" or "last".

    Returns
    -------
    dict
        Dictionary representation of the DataFrame.

    Raises
    ------
    ValueError
        - If `ids` is empty.
        - If any of the specified ID columns are not found in the DataFrame.
        - If ID values are unhashable.
        - If the DataFrame has no data (non-ID) columns.
    """
    def _ensure_hashable(x: Any, *, col: str) -> Hashable:
        """Validate x is usable as a dict key."""
        if isinstance(x, Hashable):
            return x
        raise ValueError(
            f"Unhashable ID value in column {col!r}: {x!r} (type={type(x).__name__})"
        )

    def _select_from_list(v: Any, n: int) -> Any:
        """Apply single_row/multi_row selection to an aggregated list value."""
        # Polars group_by().agg(pl.col(...)) yields lists per group.
        # Still be defensive in case of unexpected scalar.
        if not isinstance(v, list):
            if n != 1:
                # If this ever happens, it's a mismatch between __n__ and aggregation output.
                raise ValueError("Internal error: expected list-valued aggregation for multi-row group.")
            return v if single_row == "value" else [v]

        if n == 1:
            return v[0] if single_row == "value" else v

        # n > 1
        if multi_row == "list":
            return v
        if multi_row == "first":
            return v[0]
        # multi_row == "last"
        return v[-1]

    id_cols: list[str] = [ids] if isinstance(ids, str) else list(ids)
    if not id_cols:
        raise ValueError("`ids` must contain at least one column name.")

    missing = [c for c in id_cols if c not in df.columns]
    if missing:
        raise ValueError(f"ID column(s) not found in DataFrame: {missing}")

    data_cols = [c for c in df.columns if c not in id_cols]
    if not data_cols:
        raise ValueError("DataFrame must have at least one data (non-ID) column.")

    if single_col not in ("value", "dict"):
        raise ValueError(f"Invalid single_col={single_col!r}. Expected 'value' or 'dict'.")
    if single_row not in ("value", "list"):
        raise ValueError(f"Invalid single_row={single_row!r}. Expected 'value' or 'list'.")
    if multi_row not in ("list", "first", "last"):
        raise ValueError(f"Invalid multi_row={multi_row!r}. Expected 'list', 'first', or 'last'.")

    # Aggregate: one row per ID-group; each data column becomes a list; __n__ tracks group size.
    grouped = df.group_by(id_cols, maintain_order=True).agg(
        [pl.len().alias("__n__"), *[pl.col(c).alias(c) for c in data_cols]]
    )

    # Warn once if we're dropping rows via first/last for any multi-row group.
    if multi_row_warn and multi_row in ("first", "last"):
        multi = grouped.filter(pl.col("__n__") > 1)
        n_multi = multi.height
        if n_multi > 0:
            warnings.warn(
                (
                    f"DataFrame{f' {df_name!r}' if df_name else ''} contains multiple rows "
                    f"in {n_multi} ID {'group' if n_multi == 1 else 'groups'}: "
                    f"{', '.join(f"'{multi.select(pl.col(c)).row(0)[0]}'" for c in id_cols)}"
                    f". Only the {multi_row} row is kept in each group."
                ),
                UserWarning,
                stacklevel=2,
            )

    result: dict[Any, Any] = {}

    one_data_col = (len(data_cols) == 1)
    single_data_name = data_cols[0]
    drop_single_value_col_name = (single_col == "value")

    for row in grouped.iter_rows(named=True):
        n = int(row["__n__"])

        # Build key(s).
        if len(id_cols) == 1:
            key: Any = _ensure_hashable(row[id_cols[0]], col=id_cols[0])
        else:
            if flat:
                key = tuple(_ensure_hashable(row[c], col=c) for c in id_cols)
            else:
                key = None  # unused in nested mode

        # Build payload.
        if one_data_col and drop_single_value_col_name:
            payload = _select_from_list(row[single_data_name], n)
        else:
            payload = {c: _select_from_list(row[c], n) for c in data_cols}

        # Assign into flat or nested dict.
        if len(id_cols) == 1 or flat:
            result[key] = payload
        else:
            cur: dict[Any, Any] = result
            for c in id_cols[:-1]:
                kc = _ensure_hashable(row[c], col=c)
                nxt = cur.get(kc)
                if nxt is None:
                    nxt = {}
                    cur[kc] = nxt
                elif not isinstance(nxt, dict):
                    raise ValueError(
                        f"Cannot create nested mapping under key {kc!r}: existing value is not a dict."
                    )
                cur = nxt

            last_k = _ensure_hashable(row[id_cols[-1]], col=id_cols[-1])
            cur[last_k] = payload

    return result


def extract_categories(
    df: pl.DataFrame,
    categories: set[str] | None = None,
    *,
    col_name_block: str | None,
    col_name_frame: str | None,
    col_name_cat: str,
    col_name_key: str,
    col_name_values: str,
    new_col_name_block: str | None = None,
    new_col_name_frame: str | None = None,
    drop_redundant: bool = False,
) -> tuple[dict[str, pl.DataFrame], str | None, str | None]:
    """Extract tables from CIF DataFrame.

    Parameters
    ----------
    df
        CIF DataFrame to extract tables from.
        It must contain columns:
        - `col_name_cat` (str): Data category of the data item.
        - `col_name_key` (str): Data keyword of the data item.
        - `col_name_values` (List[str]): List of UTF-8 strings representing the data values.

        It may optionally contain:
        - `col_name_block` (str): Block code of the data block containing the data item.
        - `col_name_frame` (str): Frame code of the save frame containing the data item.

        It cannot contain any other columns.
    col_name_{block,frame}
        Column names for block and frame code columns.
        If the column is not present in `df`, pass `None`.
    col_name_{cat,key,values}
        Column names for data category, data keyword, and data values columns.
    new_col_name_{block,frame}
        New column names for block and frame code columns in the output tables.
        If `None`, the original column names are used.
    drop_redundant
        Whether to drop block/frame columns in the output tables
        if all rows in the original DataFrame have the same value for that column.

    Returns
    -------
    tables
        A dictionary mapping data category names to their corresponding tables
        as Polars DataFrames.
        Each table has data keywords as columns,
        and each row corresponds to a data item in that category.
        If
        - the input DataFrame contains block/frame columns,
        - and `drop_redundant` is `False` (or not all rows have the same value for that column),
        then those columns are included in the output tables as well,
        with names given by `new_col_name_block` and `new_col_name_frame`.
    out_col_name_{block,frame}
        The output column names for block and frame code columns in the tables.
        If the column was not included in the output tables, returns `None` for that column.

    Raises
    ------
    ValueError
        - If required columns are missing from `df`.
        - If `df` contains columns other than the expected ones.
        - If remaining of block/frame columns result in name conflicts with table columns.
    """

    def validate_columns():
        """Make sure only expected columns are present."""
        present_cols = set(df.columns)
        required_cols = {col_name_cat, col_name_key, col_name_values}
        if len(required_cols) < 3:
            raise ValueError(
                "col_name_cat, col_name_key, and col_name_values must be distinct column names, "
                f"got: {col_name_cat!r}, {col_name_key!r}, {col_name_values!r}."
            )
        if col_name_block is not None:
            if col_name_block in required_cols:
                raise ValueError(
                    "col_name_block must be distinct from col_name_cat, col_name_key, and col_name_values, "
                    f"got: {col_name_block!r}."
                )
            required_cols.add(col_name_block)
        if col_name_frame is not None:
            if col_name_frame in required_cols:
                raise ValueError(
                    "col_name_frame must be distinct from col_name_cat, col_name_key, and col_name_values, "
                    f"got: {col_name_frame!r}."
                )
            required_cols.add(col_name_frame)

        missing_cols = required_cols - present_cols
        if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
        extra_cols = present_cols - required_cols
        if extra_cols:
            raise ValueError(f"`df` contains unexpected columns: {extra_cols}. Expected only: {required_cols}")

    def is_redundant_col(df_: pl.DataFrame, name: str) -> bool:
        # "all rows have the same value" => n_unique == 1
        # Note: for empty df, n_unique is 0; treat as redundant (it would be constant if it existed).
        n_unique = int(df_.select(pl.col(name).n_unique()).item())
        return n_unique <= 1

    validate_columns()

    # Decide whether to keep block/frame in output tables
    keep_block = col_name_block is not None and (not drop_redundant or not is_redundant_col(df, col_name_block))
    keep_frame = col_name_frame is not None and (not drop_redundant or not is_redundant_col(df, col_name_frame))

    # Determine output names for block/frame columns
    out_block_name = (new_col_name_block or col_name_block) if keep_block else None
    out_frame_name = (new_col_name_frame or col_name_frame) if keep_frame else None
    if out_block_name is not None and out_frame_name is not None and out_block_name == out_frame_name:
        raise ValueError(
            f"Block/frame output column name conflict: both would be named {out_block_name!r}."
        )

    # Rename block/frame columns if needed
    rename_map: dict[str, str] = {}
    if keep_block:
        rename_map[col_name_block] = out_block_name  # type: ignore[arg-type]
    if keep_frame:
        rename_map[col_name_frame] = out_frame_name  # type: ignore[arg-type]
    if rename_map:
        df = df.rename(rename_map)

    # Filter by categories if specified
    if categories:
        df = df.filter(pl.col(col_name_cat).is_in(categories))

    # Partition by category
    tables: dict[str, pl.DataFrame] = {}
    for cat_value, subdf in df.partition_by(col_name_cat, include_key=False, as_dict=True).items():
        # Polars uses the partition key as the dict key; for single key it’s usually a scalar,
        # but can be a tuple depending on version/inputs.
        category_name = str(cat_value[0] if isinstance(cat_value, tuple) else cat_value)

        # Name conflict check: kept block/frame output names must not collide with keyword columns
        if out_block_name is not None or out_frame_name is not None:
            # Cast to string-ish and collect unique keywords for conflict detection
            kw = set(subdf.select(pl.col(col_name_key).cast(pl.Utf8).unique()).to_series().to_list())
            if out_block_name is not None and out_block_name in kw:
                raise ValueError(
                    f"Keeping block column would conflict with a data keyword in category {category_name!r}: "
                    f"{out_block_name!r}."
                )
            if out_frame_name is not None and out_frame_name in kw:
                raise ValueError(
                    f"Keeping frame column would conflict with a data keyword in category {category_name!r}: "
                    f"{out_frame_name!r}."
                )

        # One "row index" per item within each (kept group) by list position.
        # We assume (or earlier validation guarantees) that within a given category
        # and within each keep-group, all `values` lists have the same length.
        tables[category_name] = (
            subdf
            .with_columns(idx_data=pl.int_ranges(0, pl.col(col_name_values).list.len()))
            .explode([col_name_values, "idx_data"])
            .pivot(
                on=col_name_key,
                values=col_name_values,
            )
            .drop("idx_data")
        )

    return tables, out_block_name, out_frame_name


def extract_files(
    df: pl.DataFrame,
    files: set[Literal["data", "dict", "dict_cat", "dict_key"]] | None = None,
    *,
    col_name_frame: str,
) -> dict[str, pl.DataFrame]:
    """Extract data/dictionary parts of the CIF file.

    Parameters
    ----------
    df
        CIF DataFrame to extract from.
    files
        Parts to extract; from:
        - "data": Data file,
            i.e., data items that are directly under a data block
            (and not in any save frames).
        - "dict": Dictionary file,
            i.e., data items that are in save frames.
        - "dict_cat": Category dictionary file,
            i.e., data items that are in save frames without a frame code keyword
            (no period in the frame code).
        - "dict_key": Key dictionary file,
            i.e., data items that are in save frames with a frame code keyword
            (period in the frame code).

        If none provided, all parts found in the CIF file are extracted.
    col_name_frame
        Name of the column containing save frame codes.

    Returns
    -------
    extracted_df
        The extracted DataFrame part.
    """
    frame_code = pl.col(col_name_frame)

    is_data = frame_code.is_null()
    is_dict = ~is_data
    code_has_period = frame_code.str.contains(r"\.")

    out = {}
    for part in files or ["data", "dict", "dict_cat", "dict_key"]:
        if part == "data":
            condition = is_data
            final_columns = pl.exclude([col_name_frame])
        else:
            final_columns = pl.all()
            if part == "dict":
                condition = is_dict
            elif part == "dict_cat":
                condition = is_dict & ~code_has_period
            elif part == "dict_key":
                condition = is_dict & code_has_period
            else:
                raise ValueError(f"Invalid part: {part}")
        out[part] = df.filter(condition).select(final_columns)
    return out


def validate_content_df(
    content: DataFrameLike,
    *,
    allow_duplicate_rows: bool = False,
    require_block: bool = True,
    require_frame: bool = False,
    require_category: bool = True,
    col_name_block: str | None = "block",
    col_name_frame: str | None = "frame",
    col_name_cat: str | None = "category",
    col_name_key: str = "keyword",
    col_name_values: str = "values",
) -> pl.DataFrame:
    """Validate and normalize the content DataFrame for CIFFile.

    Parameters
    ----------
    content
        Input content DataFrame to validate and normalize.
        It can be any DataFrame-like object
        that can be converted to a Polars DataFrame with the following columns:
        - `col_name_block` (str): Block code of the data block.
        - `col_name_frame` (str | None): Frame code of the save frame.
        - `col_name_cat` (str | None): Data category of the data item.
        - `col_name_key` (str): Data keyword of the data item.
        - `col_name_values` (List[str]): List of UTF-8 strings representing the data values.
    allow_duplicate_rows
        If True, rows with the same (block, frame, category, key) identity
        will have their values concatenated instead of raising a duplicate error.
        This is useful for programmatic input where data is provided
        one-row-per-value instead of one-row-per-item.
    require_*
        Whether the corresponding column is required to be present.
    col_name_*
        Names of the columns in the DataFrame.

    Returns
    -------
    normalized_content
        A validated and normalized Polars DataFrame
        with the same columns as described above.

    Raises
    ------
    ValueError
        - If the `content` cannot be converted to a Polars DataFrame,
        - If required columns (i.e., `col_name_key`, `col_name_values`,
          plus others depending on specified `require_*` parameters) are missing,
        - If there are duplicated rows based on (block, frame, category, key) codes,
        - If block, frame, category, or key columns contain empty strings,
        - If data types of columns cannot be converted as expected,
        - If rows with same (block, frame, category) codes (those that are provided)
          have "values" lists of different lengths.
    """
    def _to_df(obj: DataFrameLike) -> pl.DataFrame:
        """Convert obj to an eager Polars DataFrame."""
        if isinstance(obj, pl.DataFrame):
            return obj
        if isinstance(obj, pl.LazyFrame):
            return obj.collect()
        try:
            schema = {
                col_name_block: pl.Utf8,
                col_name_frame: pl.Utf8,
                col_name_cat: pl.Utf8,
                col_name_key: pl.Utf8,
                col_name_values: pl.List(pl.Utf8),
            }
            # Fast path: let Polars handle common types
            return pl.DataFrame(obj, schema_overrides=schema, strict=False)
        except Exception as e:  # pragma: no cover
            # Fallback: if input is a pandas DataFrame and pyarrow is unavailable,
            # polars will raise: ImportError: pyarrow is required for converting a pandas dataframe to Polars,
            #                    unless each of its columns is a simple numpy-backed one.
            # Construct a Polars DataFrame via a plain dict of lists.
            try:
                import pandas as pd  # type: ignore
            except Exception:
                pd = None  # type: ignore
            if pd is not None and isinstance(obj, pd.DataFrame):  # type: ignore[arg-type]
                data_dict = {k: list(v) for k, v in obj.items()}
                try:
                    return pl.DataFrame(data_dict, schema_overrides=schema, strict=False)
                except Exception as ee:
                    raise ValueError(f"Could not convert 'content' to a Polars DataFrame: {ee}") from ee
            raise ValueError(f"Could not convert 'content' to a Polars DataFrame: {e}") from e

    def _require_columns(df: pl.DataFrame, cols: list[str]) -> None:
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    def _ensure_list_col(df: pl.DataFrame, col: str) -> None:
        dt = df.schema[col]
        if not isinstance(dt, pl.List):
            raise ValueError(f"Column {col!r} must be a list column (List[str]), got {dt}.")

    def _any_true(df: pl.DataFrame, expr: pl.Expr) -> bool:
        return bool(df.select(expr.any()).item())

    df = _to_df(content)

    # -------------------------
    # Required columns: must exist
    # -------------------------
    required_cols: list[str] = [col_name_key, col_name_values]
    if require_block:
        if col_name_block is None:
            raise ValueError("col_name_block must be provided when require_block=True.")
        required_cols.append(col_name_block)
    if require_frame:
        if col_name_frame is None:
            raise ValueError("col_name_frame must be provided when require_frame=True.")
        required_cols.append(col_name_frame)
    if require_category:
        if col_name_cat is None:
            raise ValueError("col_name_cat must be provided when require_category=True.")
        required_cols.append(col_name_cat)

    _require_columns(df, required_cols)

    # -------------------------
    # Type normalization / casting
    # -------------------------
    _ensure_list_col(df, col_name_values)

    try:
        exprs: list[pl.Expr] = [
            pl.col(col_name_key).cast(pl.Utf8),
            pl.col(col_name_values)
            .list.eval(pl.element().cast(pl.Utf8))
            .cast(pl.List(pl.Utf8)),
        ]
        # Optional presence: if a column exists, we cast it.
        # Required-ness only affects whether it MUST exist (handled above).
        for name in (col_name_block, col_name_frame, col_name_cat):
            if name in df.columns:
                exprs.append(pl.col(name).cast(pl.Utf8))

        df = df.with_columns(exprs)
    except Exception as e:
        raise ValueError(f"Failed to convert column dtypes as expected: {e}") from e

    # -------------------------
    # Coalesce duplicate (block, frame, category, key) rows by concatenating values
    # This allows inputs provided as one-row-per-value to be ingested.
    # -------------------------
    id_cols: list[str] = [
        c
        for c in (col_name_block, col_name_frame, col_name_cat, col_name_key)
        if c is not None and c in df.columns
    ]
    if allow_duplicate_rows and id_cols:
        df = (
            df
            .with_row_index("_row")
            .sort("_row")
            .group_by(id_cols, maintain_order=True)
            .agg(pl.col(col_name_values).alias(col_name_values))
            .with_columns(
                pl.col(col_name_values)
                .map_elements(lambda x: [e for sub in x for e in sub], return_dtype=pl.List(pl.Utf8))
            )
        )

    # -------------------------
    # Null / shape validation
    # -------------------------
    if _any_true(df, pl.col(col_name_key).is_null()):
        raise ValueError(f"Column {col_name_key!r} must not contain nulls.")

    if require_block and _any_true(df, pl.col(col_name_block).is_null()):
        raise ValueError(f"Column {col_name_block!r} must not contain nulls when require_block=True.")

    if _any_true(df, pl.col(col_name_values).is_null()):
        raise ValueError(f"Column {col_name_values!r} must be non-null in every row.")
    if _any_true(df, pl.col(col_name_values).list.eval(pl.element().is_null()).list.any()):
        raise ValueError(f"Column {col_name_values!r} must not contain null elements (expected List[str]).")

    # -------------------------
    # Empty-string validation (non-null strings must not be "")
    # -------------------------
    string_cols: list[str] = [
        c
        for c in (col_name_block, col_name_frame, col_name_cat, col_name_key)
        if c is not None and c in df.columns
    ]

    if string_cols:
        df_idx = df.with_row_index("_row")
        empty_exprs = [
            pl.col(c).is_not_null() & (pl.col(c).str.len_chars() == 0)
            for c in string_cols
        ]
        bad = df_idx.filter(pl.any_horizontal(empty_exprs)).select(["_row", *string_cols])

        if bad.height:
            rows_preview = bad.head(50).to_dicts()
            more = "" if bad.height <= 50 else f" (showing first 50 of {bad.height})"
            raise ValueError(
                f"Empty string values found in columns {string_cols}{more}. "
                f"Problematic rows: {rows_preview}"
            )

    # -------------------------
    # Length consistency within groups
    # Group by whichever of (block, frame, category) columns are PRESENT (not "required")
    # -------------------------
    group_cols = [c for c in (col_name_block, col_name_frame, col_name_cat) if c in df.columns]
    work = df.with_columns(pl.col(col_name_values).list.len().cast(pl.Int64).alias("_len"))

    if group_cols:
        lens = work.group_by(group_cols).agg(pl.col("_len").n_unique().alias("_n"))
        if bool(lens.select((pl.col("_n") > 1).any()).item()):
            raise ValueError(
                f"Rows with same ({', '.join(group_cols)}) must have {col_name_values!r} lists of the same length."
            )
    else:
        # No group columns present => all rows form one group
        if bool(work.select(pl.col("_len").n_unique() > 1).item()):
            raise ValueError(
                f"All rows must have {col_name_values!r} lists of the same length "
                f"(no {col_name_block!r}/{col_name_frame!r}/{col_name_cat!r} columns present)."
            )

    # -------------------------
    # Duplicate row validation based on identity codes
    # (block, frame, category, key) that are provided
    # -------------------------
    if id_cols:
        df_idx = df.with_row_index("_row")
        dups = (
            df_idx
            .group_by(id_cols)
            .agg(pl.col("_row").alias("_rows"), pl.len().alias("_n"))
            .filter(pl.col("_n") > 1)
        )

        if dups.height:
            preview = dups.head(50).to_dicts()
            more = "" if dups.height <= 50 else f" (showing first 50 of {dups.height})"
            raise ValueError(
                f"Duplicate rows detected based on columns {id_cols}{more}. "
                f"Conflicting groups (with row indices): {preview}"
            )

    # -------------------------
    # Return ONLY required columns (in a stable order)
    # -------------------------
    out_cols: list[str] = [
        col for col in (col_name_block, col_name_frame, col_name_cat, col_name_key, col_name_values)
        if col is not None and col in df.columns
    ]

    return df.select(out_cols)
