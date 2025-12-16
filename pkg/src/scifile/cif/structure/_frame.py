"""CIF save frame data structure."""

from typing import Literal, Iterator, Callable, Sequence

import polars as pl

from ._base import CIFBlockSkeleton
from ._category import CIFDataCategory
from ._util import extract_categories


class CIFFrame(CIFBlockSkeleton):
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
    ):
        super().__init__(
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
        )

        self._code = code

        self._category_codes: pl.Series | None = None
        self._categories: dict[str, CIFDataCategory] = {}
        return

    @property
    def code(self) -> str:
        """Save frame code."""
        return self._code

    @property
    def category_codes(self) -> pl.Series:
        """Unique data category names in the save frame."""
        if self._category_codes is None:
            self._category_codes = self._df[self._col_cat].unique()
        return self._category_codes

    def categories(self) -> Iterator[CIFDataCategory]:
        """Iterate over data categories in the save frame."""
        for category in self._get_categories().values():
            yield category

    def write(
        self,
        writer: Callable[[str], None],
        *,
        # String casting parameters
        bool_true: str = "YES",
        bool_false: str = "NO",
        null_str: Literal[".", "?"] = "?",
        null_float: Literal[".", "?"] = "?",
        null_int: Literal[".", "?"] = "?",
        null_bool: Literal[".", "?"] = "?",
        empty_str: Literal[".", "?"] = ".",
        nan_float: Literal[".", "?"] = ".",
        # Styling parameters
        always_table: bool = False,
        list_style: Literal["horizontal", "tabular", "vertical"] = "tabular",
        table_style: Literal["horizontal", "tabular-horizontal", "tabular-vertical", "vertical"] = "tabular-horizontal",
        space_items: int = 2,
        min_space_columns: int = 2,
        indent: int = 0,
        indent_inner: int = 0,
        delimiter_preference: Sequence[Literal["single", "double", "semicolon"]] = ("single", "double", "semicolon"),
    ) -> None:
        # For mmCIF keyword definitions, add leading underscore if missing
        code = self._code
        frame_code = (
            f"_{code}"
            if self._variant == "mmcif" and "." in code and not code.startswith("_") else
            code
        )
        space = " " * indent
        writer(f"{space}save_{frame_code}\n")
        for category in self.categories():
            category.write(
                writer,
                bool_true=bool_true,
                bool_false=bool_false,
                null_str=null_str,
                null_float=null_float,
                null_int=null_int,
                null_bool=null_bool,
                empty_str=empty_str,
                nan_float=nan_float,
                always_table=always_table,
                list_style=list_style,
                table_style=table_style,
                space_items=space_items,
                min_space_columns=min_space_columns,
                indent=indent + indent_inner,
                indent_inner=indent_inner,
                delimiter_preference=delimiter_preference,
            )
        writer(f"{space}save_\n")
        return

    def __iter__(self) -> Iterator[str]:
        """Iterate over data category codes in the save frame."""
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
        """Number of data categories in the save frame."""
        return len(self.category_codes)

    def __repr__(self) -> str:
        """Representation of the save frame."""
        return f"CIFFrame(code={self._code!r}, variant={self._variant!r}, categories={len(self)!r})"

    def _get_categories(self) -> dict[str, CIFDataCategory]:
        """Load all data categories in the save frame."""
        if self._categories:
            return self._categories

        category_dfs, _, _ = extract_categories(
            df=self.df,
            col_name_block=None,
            col_name_frame=None,
            col_name_cat=self._col_cat,
            col_name_key=self._col_key,
            col_name_values=self._col_values,
        )
        for cat_name, table in category_dfs.items():
            category = CIFDataCategory(
                name=cat_name,
                table=table,
                variant=self._variant,
                col_name_block=None,
                col_name_frame=None,
            )
            self._categories[cat_name] = category

        return self._categories