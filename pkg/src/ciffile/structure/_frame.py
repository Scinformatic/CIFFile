"""CIF save frame data structure."""

from typing import Literal

import polars as pl

from ._category import CIFDataCategory
from ._util import extract_categories
from ._base import CIFStructureWithCategory


class CIFFrame(CIFStructureWithCategory[CIFDataCategory]):
    """CIF file save frame."""

    def __init__(
        self,
        code: str,
        content: pl.DataFrame,
        *,
        variant: Literal["cif1", "mmcif"],
        validate: bool,
        col_name_cat: str,
        col_name_key: str,
        col_name_values: str,
        allow_duplicate_rows: bool = False,
    ):
        super().__init__(
            code=code,
            container_type="frame",
            content=content,
            variant=variant,
            validate=validate,
            require_block=False,
            require_frame=False,
            col_name_block=None,
            col_name_frame=None,
            col_name_cat=col_name_cat,
            col_name_key=col_name_key,
            col_name_values=col_name_values,
            allow_duplicate_rows=allow_duplicate_rows,
        )
        return

    def __repr__(self) -> str:
        """Representation of the save frame."""
        return f"CIFFrame(code={self._code!r}, variant={self._variant!r}, categories={len(self)!r})"

    def _get_codes(self) -> list[str]:
        """Unique data category names directly in the data block/save frame."""
        return (
            self.df
            .select(pl.col(self._col_cat).unique(maintain_order=True))
            .to_series()
            .to_list()
        )

    def _get_elements(self) -> dict[str, CIFDataCategory]:
        """Load all data categories in the save frame."""
        category_dfs, _, _ = extract_categories(
            df=self.df,
            col_name_block=None,
            col_name_frame=None,
            col_name_cat=self._col_cat,
            col_name_key=self._col_key,
            col_name_values=self._col_values,
        )
        categories = {}
        for cat_name, table in category_dfs.items():
            category = CIFDataCategory(
                code=cat_name,
                content=table,
                variant=self._variant,
                col_name_block=None,
                col_name_frame=None,
            )
            categories[cat_name] = category

        return categories

    def _get_empty_element(self) -> CIFDataCategory:
        return CIFDataCategory(
            code="",
            content=pl.DataFrame(),
            variant=self._variant,
            col_name_block=None,
            col_name_frame=None,
        )
