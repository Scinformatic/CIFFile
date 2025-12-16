"""CIF file data structure."""

from typing import Literal, Self, Iterator, Callable, Sequence

import polars as pl

from scifile.typing import DataFrameLike
from ._block import CIFBlock
from ._skel import CIFFileSkeleton


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

    def new(
        self,
        content: DataFrameLike | None = None,
        *,
        variant: Literal["cif1", "mmcif"] | None = None,
        validate: bool | None = None,
        col_name_block: str | None = None,
        col_name_frame: str | None = None,
        col_name_cat: str | None = None,
        col_name_key: str | None = None,
        col_name_values: str | None = None,
    ) -> Self:
        """Create a new `CIFFile` object.

        Parameters
        ----------
        content
            Content DataFrame for the new CIF file.
            If `None`, an empty DataFrame is used.
        variant
            CIF variant for the new CIF file.
            If `None`, the same variant as this CIF file is used.
        validate
            Whether to validate the content DataFrame for the new CIF file.
            If `None`, the same setting as this CIF file is used.
        col_name_block
            Name of the column to use for block codes in the new CIF file.
            If `None`, the same column name as this CIF file is used.
        col_name_frame
            Name of the column to use for frame codes in the new CIF file.
            If `None`, the same column name as this CIF file is used.
        col_name_cat
            Name of the column to use for category codes in the new CIF file.
            If `None`, the same column name as this CIF file is used.
        col_name_key
            Name of the column to use for key codes in the new CIF file.
            If `None`, the same column name as this CIF file is used.
        col_name_values
            Name of the column to use for value codes in the new CIF file.
            If `None`, the same column name as this CIF file is used.

        Returns
        -------
        new_cif_file
            The new `CIFFile` object.
        """
        return CIFFile(
            content=content if content is not None else pl.DataFrame(),
            variant=variant if variant is not None else self._variant,
            validate=validate if validate is not None else self._validate,
            col_name_block=col_name_block if col_name_block is not None else self._col_block,
            col_name_frame=col_name_frame if col_name_frame is not None else self._col_frame,
            col_name_cat=col_name_cat if col_name_cat is not None else self._col_cat,
            col_name_key=col_name_key if col_name_key is not None else self._col_key,
            col_name_values=col_name_values if col_name_values is not None else self._col_values,
        )

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
            ).items()
        }
        return self._block_dfs