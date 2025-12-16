"""CIF file data structure."""

from typing import Literal, Self, Iterator, Callable, Sequence

import polars as pl

from scifile.typing import DataFrameLike
from ._category import CIFDataCategory
from ._util import extract_files, extract_categories
from ._block import CIFBlock
from ._base import CIFFileSkeleton


class CIFFile(CIFFileSkeleton):
    """CIF file."""
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
        super().__init__(
            content=content,
            variant=variant,
            validate=validate,
            require_block=True,
            require_frame=False,
            col_name_block=col_name_block,
            col_name_frame=col_name_frame,
            col_name_cat=col_name_cat,
            col_name_key=col_name_key,
            col_name_values=col_name_values,
        )

        self._block_codes: pl.Series = None
        self._block_dfs: dict[str, pl.DataFrame] = {}
        self._blocks: dict[str, CIFBlock] = {}
        return

    @property
    def block_codes(self) -> pl.Series:
        """Unique block codes in the CIF file."""
        if self._block_codes is None:
            self._block_codes = self._df[self._col_block].unique()
        return self._block_codes

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
                code=cat_name,
                content=table,
                variant=self._variant,
                col_name_block=out_col_block,
                col_name_frame=out_col_frame,
            )
            for cat_name, table in dfs.items()
        }
        if len(cats) == 1:
            return next(iter(cats.values()))
        return cats

    def blocks(self) -> Iterator[CIFBlock]:
        """Iterate over data blocks in the CIF file."""
        for block_code in self.block_codes:
            yield self[block_code]

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
        """Write this CIF file in CIF format.

        Parameters
        ----------
        bool_true
            Symbol to use for boolean `True` values.
        """
        for block in self.blocks():
            block.write(
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

    def __iter__(self) -> Iterator[str]:
        """Iterate over block codes in the CIF file."""
        for block_code in self.block_codes:
            yield block_code

    def __getitem__(self, block_id: str | int) -> CIFBlock:
        """Get a data block by its block code or index."""
        block_code = (
            self.block_codes[block_id]
            if isinstance(block_id, int) else
            block_id
        )
        if block_code in self._blocks:
            return self._blocks[block_code]
        block = CIFBlock(
            code=block_code,
            content=self._get_block_dfs()[block_code],
            variant=self._variant,
            validate=False,
            col_name_frame=self._col_frame,
            col_name_cat=self._col_cat,
            col_name_key=self._col_key,
            col_name_values=self._col_values,
        )
        self._blocks[block_code] = block
        return block

    def __len__(self) -> int:
        """Number of data blocks in the CIF file."""
        return self.block_codes.shape[0]

    def __repr__(self) -> str:
        """Representation of the CIF file."""
        return f"CIFFile(type={self.type!r}, variant={self._variant!r}, blocks={len(self)!r})"

    def _get_block_dfs(self) -> dict[str, pl.DataFrame]:
        """Get DataFrames per data block in the CIF file."""
        if self._block_dfs:
            return self._block_dfs
        self._block_dfs = {
            key[0]: df
            for key, df in self.df.partition_by(
                self._col_block,
                include_key=False,
                as_dict=True,
            )
        }
        return self._block_dfs