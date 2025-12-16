from typing import Literal

import polars as pl

from ._util import validate_content_df


class CIFBlock:
    """CIF file data block."""

    def __init__(
        self,
        name: str,
        content: pl.DataFrame,
        *,
        variant: Literal["cif1", "mmcif"],
        validate: bool,
        col_name_frame: str | None,
        col_name_cat: str,
        col_name_key: str,
        col_name_values: str,
    ):
        df = validate_content_df(
            content,
            require_block=False,
            col_name_block=None,
            col_name_frame=col_name_frame,
            col_name_cat=col_name_cat,
            col_name_key=col_name_key,
            col_name_values=col_name_values,
        ) if validate else pl.DataFrame(content)
        if col_name_frame in df.columns:
            if df.select(pl.col(col_name_frame).is_null().all()).item():
                df = df.drop(col_name_frame)
                col_name_frame = None
                blocktype = "data"
            else:
                blocktype = "dict"
        else:
            col_name_frame = None
            blocktype = "data"

        self._name = name
        self._df = df
        self._variant: Literal["cif1", "mmcif"] = variant
        self._col_frame = col_name_frame
        self._col_cat = col_name_cat
        self._col_key = col_name_key
        self._col_values = col_name_values
        self._blocktype = blocktype
        return

    @property
    def name(self) -> str:
        """Data block name."""
        return self._name

    @property
    def type(self) -> Literal["data", "dict"]:
        """Type of CIF block

        Either:
        - "data": Data block contains no save frames; all data items are directly under the block.
        - "dict": Data block contains save frames; some data items are under save frames.
        """
        return self._blocktype

    @property
    def df(self) -> pl.DataFrame:
        """Data block content as DataFrame."""
        return self._df

