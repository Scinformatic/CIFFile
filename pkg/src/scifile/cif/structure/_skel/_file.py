"""CIF file data structure base class."""

from typing import Literal, Self

import polars as pl

from .._util import extract_categories, extract_files
from .._category import CIFDataCategory
from ._block import CIFBlockSkeleton


class CIFFileSkeleton(CIFBlockSkeleton):
    """CIF file data structure base class."""

    def __init__(
        self,
        *,
        col_name_frame: str | None,
        **kwargs,
    ) -> None:
        super().__init__(col_name_frame=col_name_frame, **kwargs)
        if col_name_frame in self.df.columns:
            if self.df.select(pl.col(col_name_frame).is_null().all()).item():
                self._df = self.df.drop(col_name_frame)
                col_name_frame = None
                filetype = "data"
            else:
                filetype = "dict"
        else:
            col_name_frame = None
            filetype = "data"

        self._col_frame = col_name_frame
        self._filetype = filetype

        self._parts: dict[Literal["data", "dict", "dict_cat", "dict_key"], pl.DataFrame] = {}
        return

    @property
    def type(self) -> Literal["data", "dict"]:
        """Type of the CIF file/block.

        Either:
        - "data": File contains no save frames; all data items are directly under data blocks.
        - "dict": File contains save frames; some data items are under save frames.
        """
        return self._filetype

    def category(
        self,
        *category: str,
        col_name_block: str | None = "_block",
        col_name_frame: str | None = "_frame",
        drop_redundant: bool = True,
    ) -> CIFDataCategory | dict[str, CIFDataCategory]:
        """Extract data category tables from all data blocks/save frames.

        Parameters
        ----------
        *category
            Names of data categories to extract.
            If none provided, all categories found in the CIF file are extracted.
        col_name_block
            Name of the column to use for block codes in the output tables.
        col_name_frame
            Name of the column to use for frame codes in the output tables.
        drop_redundant
            Whether to drop block/frame code columns
            if they have the same value for all rows.

        Returns
        -------
        data_category_tables
            A single `CIFDataCategory` if only one category is requested,
            or a dictionary of `CIFDataCategory` objects
            keyed by category name otherwise.
        """
        dfs, out_col_block, out_col_frame = extract_categories(
            self._df,
            categories=set(category),
            col_name_block=self._col_block,
            col_name_frame=self._col_frame,
            col_name_cat=self._col_cat,
            col_name_key=self._col_key,
            col_name_values=self._col_values,
            new_col_name_block=col_name_block,
            new_col_name_frame=col_name_frame,
            drop_redundant=drop_redundant,
        )
        cats = {
            cat_name: CIFDataCategory(
                code=cat_name,
                content=table,
                variant=self._variant,
                col_name_block=out_col_block,
                col_name_frame=out_col_frame,
            )
            for cat_name, table in dfs.items()
        }
        if len(cats) == 1:
            return next(iter(cats.values()))
        return cats

    def part(
        self,
        *part: Literal["data", "dict", "dict_cat", "dict_key"],
    ) -> Self | None | dict[str, Self | None]:
        """Isolate data/dictionary parts of the file.

        Parameters
        ----------
        *part
            Parts to extract; from:
            - "data": Data file,
              i.e., data items that are directly under data blocks
              (and not in any save frames).
            - "dict": Dictionary file,
              i.e., data items that are in save frames.
            - "dict_cat": Category dictionary file,
              i.e., data items that are in save frames without a frame code keyword
              (no period in the frame code).
            - "dict_key": Key dictionary file,
              i.e., data items that are in save frames with a frame code keyword
              (period in the frame code).

            If none provided, all parts found are extracted.

        Returns
        -------
        isolated_parts
            A single object like self if only one part is requested,
            or a dictionary of objects
            keyed by part name otherwise.
        """
        parts = set(part) if part else {"data", "dict", "dict_cat", "dict_key"}

        out = {}
        for p in parts:
            part_df = self._get_part(p)
            part_obj = self.new(
                content=part_df,
                validate=False,
            ) if not part_df.is_empty() else None
            out[p] = part_obj

        if len(parts) == 1:
            return out[next(iter(parts))]
        return out

    def _get_part(self, part: Literal["data", "dict", "dict_cat", "dict_key"]) -> pl.DataFrame:
        """Get data/dictionary part of the structure.

        Parameters
        ----------
        part
            Part to extract; from:
            - "data": Data items that are directly under the data block
            - "dict": Dictionary items that are directly under the data block
            - "dict_cat": Category dictionary items
            - "dict_key": Key dictionary items

        Returns
        -------
        pl.DataFrame
            Extracted part of the data block.
        """
        file_part = self._parts.get(part)
        if file_part is not None:
            return file_part

        self._parts = {
            "data": self._df,
            "dict": pl.DataFrame(),
            "dict_cat": pl.DataFrame(),
            "dict_key": pl.DataFrame(),
        } if self._col_frame is None else extract_files(
            df=self._df,
            col_name_frame=self._col_frame,
        )
        return self._parts[part]