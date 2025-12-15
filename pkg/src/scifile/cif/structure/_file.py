from typing import Literal, Any

import polars as pl

from scifile.typing import DataFrameLike
from scifile.cif.typing import BlockCode, FrameCode, DataCategory, DataKeyword, DataValues
from ._category import CIFDataCategory
from ._util import extract, extract_tables, validate_content_df


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
        self._variant = variant
        self._col_block = col_name_block
        self._col_frame = col_name_frame
        self._col_cat = col_name_cat
        self._col_key = col_name_key
        self._col_values = col_name_values
        self._filetype = filetype

        self._block_codes: pl.Series = None

        return

    @property
    def filetype(self) -> Literal["data", "dict"]:
        """Type of CIF file

        Either:
        - "data": The file contains no save frames; all data items are directly under data blocks.
        - "dict": The file contains save frames; some data items are under save frames.
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

    def extract(
        self,
        part: Literal["data", "def", "def_cat", "def_key", "all"] = "data",
        reduce: bool = True,
    ) -> CIFDataFile | CIFDictFile | DDL2CIFCatDefFile | dict[
        str, CIFDataFile | CIFDictFile | DDL2CIFCatDefFile
    ]:
        """Extract part of the CIF file.

        Parameters
        ----------
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
            - "all": All parts; returns a dictionary with keys "data", "def_cat", and "def_key".
        reduce
            Whether to reduce the result to a single data block if there is only one.

        Returns
        -------
        extracted_part
            The extracted part(s) of the CIF file.
        """
        if reduce and self.block_count == 1:
            return self.block().extract(part=part)

        if part == "all":
            parts = ("data", "def_cat", "def_key")
            sub_dfs = [extract(df=self._df, part=part) for part in parts]
            return {
                part: datastruct(df=sub_df)
                for part, datastruct, sub_df in zip(
                    parts,
                    (CIFDataFile, DDL2CIFCatDefFile, CIFDictFile),
                    sub_dfs,
                    strict=True,
                )
            }

        sub_df = extract(df=self._df, part=part)
        if part == "data":
            return CIFDataFile(df=sub_df)
        if part in ("def", "def_key"):
            return CIFDictFile(df=sub_df)
        if part == "def_cat":
            return DDL2CIFCatDefFile(df=sub_df)
        raise ValueError(f"Invalid part: {part}")

    def table(
        self,
        *category: str,
        col_name_block: str | None = "_block",
        col_name_frame: str | None = "_frame",
    ):
        return extract_tables(
            self._df,
            categories=set(category),
            col_name_block=self._col_block,
            col_name_frame=self._col_frame,
            col_name_cat=self._col_cat,
            col_name_key=self._col_key,
            col_name_values=self._col_values,
            new_col_name_block=col_name_block,
            new_col_name_frame=col_name_frame,
            drop_redundant=False,
        )

