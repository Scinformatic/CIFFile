"""CIF block-like data structure."""

from __future__ import annotations

from typing import TYPE_CHECKING

from typing import Iterator

import polars as pl

from ._category import CIFDataCategory


if TYPE_CHECKING:
    from typing import Literal
    from scifile.typing import DataFrameLike


class CIFBlockLike:
    """CIF block-like data structure base class."""

    def __init__(self, *, code: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._code = code

        self._category_codes: pl.Series | None = None
        self._categories: dict[str, CIFDataCategory] = {}
        return

    @property
    def code(self) -> str:
        """Block/frame code."""
        return self._code

    @property
    def category_codes(self) -> pl.Series:
        """Unique data category names in the data block/save frame."""
        if self._category_codes is None:
            self._category_codes = self._df[self._col_cat].unique()
        return self._category_codes

    def categories(self) -> Iterator[CIFDataCategory]:
        """Iterate over data categories in the data block/save frame."""
        for category in self._get_categories().values():
            yield category

    def __iter__(self) -> Iterator[str]:
        """Iterate over data category codes in the data block/save frame."""
        for cat_code in self.category_codes:
            yield cat_code

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
            codes = self.category_codes[category_id].to_list()
        else:
            raise TypeError("category_id must be str, int, tuple, or slice")

        categories = self._get_categories()
        out = [categories[k] for k in codes]

        if single:
            return out[0]
        return out

    def __len__(self) -> int:
        """Number of data categories directly under this data block/save frame."""
        return len(self.category_codes)
