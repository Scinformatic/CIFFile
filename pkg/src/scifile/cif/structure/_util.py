from typing import Literal, Sequence

import polars as pl


def dataframe_per_table(
    df: pl.DataFrame,
    col_name__table_id: str = "data_name_category",
    col_name__col_id: str = "data_name_keyword",
    col_name__values: str = "data_values",
    col_name__other_ids: Sequence[str] = (
        "block_code",
        "frame_code_category",
        "frame_code_keyword",
    ),
) -> dict[str, pl.DataFrame]:

    def pivot_single_table(tbl: pl.DataFrame) -> pl.DataFrame:
        return (
            tbl
            .with_columns(
                idx_data=pl.int_ranges(0, pl.col(col_name__values).list.len())
            )
            .explode([col_name__values, "idx_data"])
            .pivot(
                on=col_name__col_id,
                index=[*col_name__other_ids, "idx_data"],
                values=col_name__values,
            )
            .drop("idx_data")
        )

    return {
        table_id[0]: pivot_single_table(subdf)
        for table_id, subdf in df.group_by(col_name__table_id)
    }


def extract(
    df: pl.DataFrame,
    part: Literal["data", "def", "def_cat", "def_key"] = "data"
) -> pl.DataFrame:
    """Extract part of the CIF DataFrame.

    Parameters
    ----------
    df
        CIF DataFrame to extract from.
    part
        Part to extract; one of:
        - "data": Data,
          i.e., data items that are directly under a data block
          (and not in any save frames).
        - "def": Definitions,
          i.e., data items that are in save frames.
        - "def_cat": Category definitions,
          i.e., data items that are in save frames without a frame code keyword
          (no period in the frame code).
        - "def_key": Key definitions,
          i.e., data items that are in save frames with a frame code keyword
          (period in the frame code).

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
