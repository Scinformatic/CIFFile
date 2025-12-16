from typing import Literal, Callable, Sequence

import polars as pl


class CIFDataCategory:
    """CIF file data category."""

    def __init__(
        self,
        name: str,
        table: pl.DataFrame,
        *,
        variant: Literal["cif1", "mmcif"],
        col_name_block: str | None = None,
        col_name_frame: str | None = None,
    ):

        self._name = name
        self._table = table
        self._variant = variant
        self._col_block = col_name_block
        self._col_frame = col_name_frame
        return

    @property
    def name(self) -> str:
        """Data category name."""
        return self._name

    @property
    def table(self) -> pl.DataFrame:
        """Data category table."""
        return self._table

    def write(
        self,
        writer: Callable[[str], None],
        *,
        loop: bool = False,
    ) -> None:
        """Write CIF data category to writer.

        Parameters
        ----------
        writer
            A callable that takes a string and writes it to the desired output.
            This could be a file write method or any other string-consuming function.
            For example, you can create a list and pass its `append` method
            to collect the output chunks into the list.
            The whole CIF content can then be obtained by joining the list elements,
            i.e., `''.join(output_list)`.
        loop
            Whether to write the data category in loop format
            even if it contains only a single row.
            When `False` (default),
            single-row categories are written in simple key-value format.
        """
        # Write category data items
        for row in self._table.iter_rows(named=True):
            key = row[self._col_key]
            values = row[self._col_values]
            writer(f"_{self._name}.{key} {values}\n")

    def __repr__(self) -> str:
        return f"CIFDataCategory(name={self._name!r}, shape={self._table.shape})"
