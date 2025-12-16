"""CIF block data structure."""

from typing import Literal

import polars as pl

from ._base import CIFFileSkeleton


class CIFBlock(CIFFileSkeleton):
    """CIF file data block."""

    def __init__(
        self,
        code: str,
        content: pl.DataFrame,
        *,
        variant: Literal["cif1", "mmcif"],
        validate: bool,
        col_name_frame: str | None,
        col_name_cat: str,
        col_name_key: str,
        col_name_values: str,
    ):
        super().__init__(
            content=content,
            variant=variant,
            validate=validate,
            require_block=False,
            require_frame=False,
            col_name_block=None,
            col_name_frame=col_name_frame,
            col_name_cat=col_name_cat,
            col_name_key=col_name_key,
            col_name_values=col_name_values,
        )

        self._code = code
        return

    @property
    def code(self) -> str:
        """Data block code."""
        return self._code
