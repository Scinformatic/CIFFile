from typing import Literal, Sequence

import polars as pl

from scifile.typing import DataFrameLike


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
    for cat_value, subdf in df.partition_by(col_name_cat, as_dict=True).items():
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
            .drop(col_name_cat)
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
    col_name_frame: str | None = "frame_code",
) -> dict[str, pl.DataFrame]:
    """data/dictionary parts of the CIF file.

    Parameters
    ----------
    df
        CIF DataFrame to extract from.
    *file
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

    Returns
    -------
    extracted_df
        The extracted DataFrame part.
    """
    frame_cat = pl.col("frame_code_category")
    frame_key = pl.col("frame_code_keyword")
    condition = frame_cat.is_null() if part == "data" else frame_cat.is_not_null()

    if part == "data":
        final_columns = pl.exclude(["frame_code_category", "frame_code_keyword"])
    elif part == "def":
        final_columns = pl.all()
    elif part == "def_cat":
        condition &= frame_key.is_null()
        final_columns = pl.exclude(["frame_code_keyword"])
    elif part == "def_key":
        condition &= frame_key.is_not_null()
        final_columns = pl.all()
    else:
        raise ValueError(f"Invalid part: {part}")

    return df.filter(condition).select(final_columns)


def validate_content_df(
    content: DataFrameLike,
    *,
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
            return pl.DataFrame(obj)
        except Exception as e:  # pragma: no cover
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
    # Return ONLY required columns (in a stable order)
    # -------------------------
    out_cols: list[str] = [
        col for col in (col_name_block, col_name_frame, col_name_cat, col_name_key, col_name_values)
        if col is not None
    ]
    return df.select(out_cols)
