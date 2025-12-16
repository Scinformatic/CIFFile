from typing import Literal, Any, Self

import polars as pl

from scifile.typing import DataFrameLike
from scifile.cif.typing import BlockCode, FrameCode, DataCategory, DataKeyword, DataValues
from ._category import CIFDataCategory
from ._util import extract_files, extract_categories, validate_content_df


class CIFFile:
    """CIF file data structure.

    Parameters
    ----------

    """
    def __init__(
        self,
        content: DataFrameLike,
        *,
        variant: Literal["cif1", "mmcif"],
        validate: bool,
        col_name_block: str,
        col_name_frame: str | None,
        col_name_cat: str,
        col_name_key: str,
        col_name_values: str,
    ):
        df = validate_content_df(
            content,
            col_name_block=col_name_block,
            col_name_frame=col_name_frame,
            col_name_cat=col_name_cat,
            col_name_key=col_name_key,
            col_name_values=col_name_values,
        ) if validate else pl.DataFrame(content)
        if col_name_frame in df.columns:
            if df.select(pl.col(col_name_frame).is_null().all()).item():
                df = df.drop(col_name_frame)
                col_name_frame = None
                filetype = "data"
            else:
                filetype = "dict"
        else:
            col_name_frame = None
            filetype = "data"

        self._df = df
        self._variant: Literal["cif1", "mmcif"] = variant
        self._col_block = col_name_block
        self._col_frame = col_name_frame
        self._col_cat = col_name_cat
        self._col_key = col_name_key
        self._col_values = col_name_values
        self._filetype = filetype

        self._block_codes: pl.Series = None

        return

    @property
    def type(self) -> Literal["data", "dict"]:
        """Type of CIF file

        Either:
        - "data": File contains no save frames; all data items are directly under data blocks.
        - "dict": File contains save frames; some data items are under save frames.
        """
        return self._filetype

    @property
    def df(self) -> pl.DataFrame:
        """DataFrame representation of the CIF file."""
        return self._df

    @property
    def block_codes(self) -> pl.Series:
        """Unique block codes in the CIF file."""
        if self._block_codes is None:
            self._block_codes = self._df[self._col_block].unique()
        return self._block_codes

    @property
    def block_count(self) -> int:
        """Number of data blocks in the CIF file."""
        return self.block_codes.shape[0]

    def file(
        self,
        *file: Literal["data", "dict", "dict_cat", "dict_key"],
    ) -> Self | None | dict[str, Self | None]:
        """Isolate data/dictionary parts of the CIF file.

        Parameters
        ----------
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
        isolated_files
            A single `CIFFile` if only one part is requested,
            or a dictionary of `CIFFile` objects
            keyed by part name otherwise.
        """
        filetypes = set(file) if file else {"data", "dict", "dict_cat", "dict_key"}
        if self.type == "data":
            # Only data part exists
            out = {
                "data": self,
                "dict": None,
                "dict_cat": None,
                "dict_key": None,
            }
            if len(filetypes) == 1:
                return out[next(iter(filetypes))]
            return {part: out[part] for part in filetypes}

        dfs = extract_files(
            df=self._df,
            files=filetypes,
            col_name_frame=self._col_frame,
        )

        files = {
            part: (CIFFile(
                content=sub_df,
                variant=self._variant,
                validate=False,
                col_name_block=self._col_block,
                col_name_frame=self._col_frame,
                col_name_cat=self._col_cat,
                col_name_key=self._col_key,
                col_name_values=self._col_values,
            ) if not sub_df.is_empty() else None)
            for part, sub_df in dfs.items()
        }

        if len(filetypes) == 1:
            return files[next(iter(filetypes))]
        return files

    def block(self, block: str | int = 0):
        """Extract a data block by its block code or index.

        Parameters
        ----------
        block
            Block code (str) or index (int) of the data block to extract.

        Returns
        -------
        data_block
            The extracted data block.
        """
        block_code = (
            self.block_codes[block]
            if isinstance(block, int) else
            block
        )
        return DDL2CIFBlock(
            block_code=block_code,
            df=self.df.filter(pl.col("block_code") == block_code).select(pl.exclude("block_code")),
        )

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
                name=cat_name,
                table=table,
                variant=self._variant,
                col_name_block=out_col_block,
                col_name_frame=out_col_frame,
            )
            for cat_name, table in dfs.items()
        }
        if len(cats) == 1:
            return next(iter(cats.values()))
        return cats
