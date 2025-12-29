"""CIF block data structure."""

from typing import Literal, Self

import polars as pl

from ciffile.typing import DataFrameLike
from ciffile.validation import dictionary as to_validator_dict
from ._base import CIFStructureWithFrame
from ._util import extract_categories
from ._category import CIFDataCategory
from ._frames import CIFBlockFrames


class CIFBlock(CIFStructureWithFrame[CIFDataCategory]):
    """CIF file data block."""

    def __init__(
        self,
        code: str,
        content: DataFrameLike,
        *,
        variant: Literal["cif1", "mmcif"],
        validate: bool,
        col_name_frame: str | None,
        col_name_cat: str,
        col_name_key: str,
        col_name_values: str,
        col_name_block: None = None,
        allow_duplicate_rows: bool = False,
    ):
        super().__init__(
            code=code,
            container_type="block",
            content=content,
            variant=variant,
            validate=validate,
            require_block=False,
            require_frame=False,
            col_name_block=col_name_block,
            col_name_frame=col_name_frame,
            col_name_cat=col_name_cat,
            col_name_key=col_name_key,
            col_name_values=col_name_values,
            allow_duplicate_rows=allow_duplicate_rows,
        )
        self._frames: CIFBlockFrames | None = None
        return

    @property
    def frames(self) -> CIFBlockFrames:
        """Save frames in the data block."""
        if self._frames is None:
            self._frames = CIFBlockFrames(
                content=self._get_part("dict"),
                variant=self._variant,
                col_name_frame=self._col_frame,
                col_name_cat=self._col_cat,
                col_name_key=self._col_key,
                col_name_values=self._col_values,
            )
        return self._frames

    def to_validator_dict(
        self,
        *,
        variant: Literal["ddl2"] = "ddl2"
    ) -> dict:
        return to_validator_dict(self, variant=variant)

    def __repr__(self) -> str:
        return f"CIFBlock(code={self.code!r}, type={self.type!r}, variant={self._variant!r}, categories={len(self)}, frames={len(self.frames)})"

    def _get_codes(self) -> list[str]:
        """Unique data category names directly in the data block/save frame."""
        df = self.df
        if self._col_frame is not None:
            df = df.filter(pl.col(self._col_frame).is_null())
        return (
            df
            .select(pl.col(self._col_cat).unique(maintain_order=True))
            .to_series()
            .to_list()
        )

    def _get_elements(self) -> dict[str, CIFDataCategory]:
        """Load all data categories directly in the data block."""
        data_df = self._get_part("data")
        category_dfs, _, _ = extract_categories(
            df=data_df,
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