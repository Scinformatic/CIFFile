"""CIF block-like data structure."""

from __future__ import annotations

from typing import TYPE_CHECKING, overload

import polars as pl


if TYPE_CHECKING:
    from typing import Iterator
    from ._category import CIFDataCategory


class CIFBlockLike:
    """CIF block-like data structure base class."""

    def __init__(self, *, code: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._code = code

        self._category_codes: list[str] = []
        self._categories: dict[str, CIFDataCategory] = {}
        return

    @property
    def code(self) -> str:
        """Block/frame code."""
        return self._code

    @property
    def category_codes(self) -> list[str]:
        """Unique data category names directly in the data block/save frame."""
        if not self._category_codes:
            df = self.df
            if self._col_frame is not None:
                df = df.filter(pl.col(self._col_frame).is_null())
            self._category_codes = (
                df
                .select(pl.col(self._col_cat).unique(maintain_order=True))
                .to_series()
                .to_list()
            )
        return self._category_codes

    def __iter__(self) -> Iterator[CIFDataCategory]:
        """Iterate over data categories in the data block/save frame."""
        for category_code in self.category_codes:
            yield self[category_code]

    @overload
    def __getitem__(self, category_id: str | int) -> CIFDataCategory: ...
    @overload
    def __getitem__(self, category_id: tuple[str | int, ...] | slice[int]) -> list[CIFDataCategory]: ...
    def __getitem__(
        self,
        category_id: str | int | tuple[str | int, ...] | slice[int]
    ) -> CIFDataCategory | list[CIFDataCategory]:
        """Get a data category by its code or index."""
        if isinstance(category_id, str | int):
            category_id = (category_id,)
            single = True
        else:
            single = False

        if isinstance(category_id, tuple):
            codes = [
                self.category_codes[cat_id]
                if isinstance(cat_id, int)
                else cat_id
                for cat_id in category_id
            ]
        elif isinstance(category_id, slice):
            codes = self.category_codes[category_id]
        else:
            raise TypeError("category_id must be str, int, tuple, or slice")

        categories = self._get_categories()
        out = [categories[k] for k in codes]

        if single:
            return out[0]
        return out

    def __contains__(self, category_code: str) -> bool:
        """Check if a data category with the given code exists in this data block/save frame."""
        return category_code in self.category_codes

    def __len__(self) -> int:
        """Number of data categories directly in this data block/save frame."""
        return len(self.category_codes)
