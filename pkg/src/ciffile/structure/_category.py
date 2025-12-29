"""CIF category data structure."""

from typing import Literal, Callable, Sequence

import polars as pl

from ciffile.writer import category as write_category

from ciffile.typing import DataFrameLike
from ._base import CIFStructureWithItem
from ._item import CIFDataItem


class CIFDataCategory(CIFStructureWithItem[CIFDataItem]):
    """CIF file data category."""

    def __init__(
        self,
        code: str,
        content: DataFrameLike,
        *,
        variant: Literal["cif1", "mmcif"],
        col_name_block: str | None = None,
        col_name_frame: str | None = None,
    ):
        super().__init__(
            code=code,
            container_type="category",
            content=content,
            variant=variant,
        )
        self._df = self._df.select(sorted(self._df.columns))
        self._col_block = col_name_block
        self._col_frame = col_name_frame

        self._description: str | None = None
        self._groups: dict[str, dict[str, str]] | None = None
        self._keys: list[str] | None = None
        self._item_names: list[str] | None = None
        return

    @property
    def item_names(self) -> list[str]:
        """Full names of the data items in this data category."""
        if self._item_names is None:
            self._item_names = [item.name for item in self]
        return self._item_names

    @CIFStructureWithItem.df.setter
    def df(self, new_df: pl.DataFrame) -> None:
        """Re-set the underlying DataFrame for this data category."""
        cols = []
        if self._col_block is not None and self._col_block in new_df.columns:
            cols.append(self._col_block)
        if self._col_frame is not None and self._col_frame in new_df.columns:
            cols.append(self._col_frame)
        if self.keys is not None:
            cols.extend(sorted(self.keys))
        remaining_cols = sorted([col for col in new_df.columns if col not in cols])
        all_cols = cols + remaining_cols
        new_df = new_df.select(all_cols)

        if self.keys:
            new_df = new_df.sort(by=self.keys, nulls_last=True)

        self._df = new_df

        # Refresh items
        self.refresh()
        self._item_names = None
        return

    @property
    def description(self) -> str | None:
        """Description of this data category, if available."""
        return self._description

    @description.setter
    def description(self, desc: str | None) -> None:
        """Set the description of this data category."""
        self._description = desc
        return

    @property
    def groups(self) -> dict[str, dict[str, str]] | None:
        """Groups this data category belongs to, if available.

        This is a mapping of group IDs to dictionaries with keys:
        - "description": Description of the category group.
        - "parent_id": Parent category group ID, if available.
        """
        return self._groups

    @groups.setter
    def groups(self, groups: dict[str, dict[str, str]] | None) -> None:
        """Set the groups this data category belongs to."""
        self._groups = groups
        return

    @property
    def keys(self) -> list[str] | None:
        """Data item codes (column names) corresponding to category keys, if available."""
        return self._keys

    @keys.setter
    def keys(self, keys: list[str] | None) -> None:
        """Set the data item codes (column names) corresponding to category keys."""
        for key in keys or []:
            if key not in self.codes:
                raise ValueError(f"Key {key!r} not found in category {self._code!r} codes.")
        df = self._df
        if keys:
            df = df.sort(by=keys, nulls_last=True)
        self._keys = keys
        self.df = df  # re-set to update column order
        return

    def __repr__(self) -> str:
        return f"CIFDataCategory(name={self._code!r}, shape={self._df.shape})"

    def _get_elements(self) -> dict[str, CIFDataItem]:
        """Generate CIFDataItem objects for each data item (column)."""
        return {
            keyword: CIFDataItem(
                code=keyword,
                name=keyword if self._variant == "cif1" else f"{self._code}.{keyword}",
                content=self._df[keyword],
            )
            for keyword in self.codes
        }

    def _get_codes(self) -> list[str]:
        """Get codes of the data items (columns) in this data category."""
        return [
            col for col in self._df.columns
            if col not in (self._col_block, self._col_frame)
        ]

    def _get_empty_element(self) -> CIFDataItem:
        return CIFDataItem(
            code="",
            name="",
            content=pl.Series([], dtype=pl.Null),
        )

    def _write(
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
        """Write this data category in CIF format."""
        exclude_columns = [col for col in (self._col_block, self._col_frame) if col is not None]
        df = self.df.select(pl.exclude(exclude_columns))
        if self._variant == "mmcif":
            # Set column names to full data names
            df = df.select(pl.all().name.prefix(f"_{self._code}."))
        else:
            # CIF1: prefix column names with underscore only
            df = df.select(pl.all().name.prefix("_"))
        write_category(
            df,
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
            indent=indent,
            indent_inner=indent_inner,
            delimiter_preference=delimiter_preference,
        )
        return
