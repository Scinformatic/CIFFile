from typing import Literal

import polars as pl

from ._exception import CIFValidationError


class CIFFileValidator:
    def __init__(self, df: pl.DataFrame):
        self._df = df
        self._df_ids = df.select(pl.exclude(["data_values", "loop_id"]))
        return

    def validate(self, ddl_version: Literal[1, 2] = 2) -> list[CIFValidationError]:
        empty_data_id = not self.has_empty_data_identifier()
        duplicated = not self.has_duplicated_address()
        table_columns = self.table_columns_have_same_length()
        cat_columns = self.data_category_columns_have_same_length()
        null = not self.has_null()
        loops = self.loops_have_same_category()
        ddl = self.is_ddl2_conformant() if ddl_version == 2 else self.is_ddl1_conformant()
        return all([empty_data_id, duplicated, table_columns, cat_columns, null, loops, ddl])

    def has_empty_data_identifier(
        self, dim: Literal["elem", "col", "row", "df"] = "df"
    ) -> pl.DataFrame | pl.Series | bool:
        if dim == "df":
            return self._df_ids.select((pl.any(pl.all() == "")).any())[0, 0]
        if dim == "row":
            return self._df_ids.select(pl.any(pl.all() == ""))
        if dim == "col":
            return self._df_ids.select((pl.all() == "").any())
        if dim == "elem":
            return self._df_ids.select(pl.all() == "")
        raise ValueError

    def has_duplicated_address(self, dim: Literal["row", "df"] = "df"):
        mask = self._df_ids.is_duplicated()
        if dim == "row":
            return mask
        if dim == "df":
            return mask.any()
        raise ValueError

    def frame_codes_are_unique_within_block(self, per_block: bool = False):
        """Check whether frame codes are unique within each data block.

        Parameters
        ----------
        per_block : bool, default: False
            If True, a DataFrame is returned with one row per data block, indicating whether
            the frame codes within that block are unique (True) or not (False). Otherwise, a single
            boolean value is returned indicating whether the frame codes are unique across all
            data blocks (True) or not (False).

        Notes
        -----
        According to the CIF specification,
        frame codes (i.e., the combination of category and keyword)
        should be unique within each data block.
        """
        df_per_block = (
            self._df_ids.groupby("block_code")
            .agg(
                (pl.col("frame_code_category").concat(pl.lit(".")).concat(pl.col("frame_code_keyword")))
                .is_duplicated()
                .any()
                .alias("has_duplicated_frame_codes")
            )
        )
        if per_block:
            return df_per_block
        return df_per_block["has_duplicated_frame_codes"].any()


    def has_null(self, per_row: bool = False):
        series: pl.Series = self._df.select(
            pl.any(
                pl.exclude(
                    ["frame_code_category", "frame_code_keyword", "data_name_keyword"]
                ).is_null()
            ).alias("mask")
        )["mask"]
        if per_row:
            return series
        return series.any()

    def table_columns_have_same_length(self, per_table: bool = False):
        df_per_loop = (
            self._df.with_columns(pl.col("data_value").arr.lengths().alias("list_lengths"))
            .groupby("loop_id")
            .agg((pl.col("list_lengths").n_unique() == 1).alias("has_same_length"))
        )
        if per_table:
            return df_per_loop
        return df_per_loop["has_same_length"].all()

    def data_category_columns_have_same_length(self, per_category: bool = False):
        df_per_category = (
            self._df.with_columns(pl.col("data_value").arr.lengths().alias("list_lengths"))
            .groupby(
                ["block_code", "frame_code_category", "frame_code_keyword", "data_name_category"]
            )
            .agg((pl.col("list_lengths").n_unique() == 1).alias("has_same_length"))
        )
        if per_category:
            return df_per_category
        return df_per_category["has_same_length"].all()

    def loops_have_same_category(self, per_loop: bool = False):
        df_per_loop = (
            self._df.filter(pl.col("loop_id") > 0)
            .groupby("loop_id")
            .agg((pl.col("data_name_category").n_unique() == 1).alias("has_same_category"))
        )
        if per_loop:
            return df_per_loop
        return df_per_loop["has_same_category"].all()

    def is_ddl2_conformant(self, per_data_name: bool = False) -> [pl.Series, bool]:
        """
        Check whether data names conform to Dictionary Definition Language Version 2 (DDL2) standard.

        According to the DDL2 standard, data names must consist of a category name and a keyword name,
        separated by a '.' character. Notice that there is no constraint on frame codes, i.e. they can
        either have a category name and a keyword name like data names, or just a category name.

        Parameters
        ----------
        per_data_name : bool, default: False
            If True, a polars Series is returned, indicating whether each data name conforms (True) to
            the DDL2 standard or not (False). Otherwise, a single boolean value is returned indicating
            whether all data names are conformant (True) or not (False).

        Returns
        -------
        polars.Series or bool
        """
        # If the data name contains no '.', the parser assigns a None to the keyword name.
        #  Therefore, we can just check whether the `data_name_keyword` column has null values.
        series_per_data_name: pl.Series = self._df_ids.select(
            pl.col("data_name_keyword").is_not_null()
        )["data_name_keyword"]
        if per_data_name:
            return series_per_data_name
        return series_per_data_name.all()

    def is_ddl1_conformant(self, per_data_name: bool = False) -> [pl.Series, bool]:
        series_per_data_name: pl.Series = self._df.select(
            pl.all(pl.col(["frame_code_keyword", "data_name_keyword"]).is_null()).alias("mask")
        )["mask"]
        if per_data_name:
            return series_per_data_name
        return series_per_data_name.all()
