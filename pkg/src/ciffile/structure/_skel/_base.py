"""CIF data structure base class."""

from abc import abstractmethod, ABCMeta
from typing import Callable, Literal

import polars as pl

from ciffile.typing import DataFrameLike
from .._util import dataframe_to_dict


class CIFSkeleton(metaclass=ABCMeta):
    """CIF data structure base class."""

    def __init__(
        self,
        *,
        content: DataFrameLike,
        variant: Literal["cif1", "mmcif"],
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)
        self._df = content if isinstance(content, pl.DataFrame) else pl.DataFrame(content)
        self._variant: Literal["cif1", "mmcif"] = variant
        return

    def to_id_dict(
        self,
        ids: str | list[str],
        flat: bool = False,
        single_col: Literal["value", "dict"] = "value",
        single_row: Literal["value", "list"] = "value",
        multi_row: Literal["list", "first", "last"] = "list",
        multi_row_warn: bool = False,
    ) -> dict:
        """Convert DataFrame representation to dictionary.

        Parameters
        ----------
        ids
            Column name(s) to use as dictionary keys.
            ID values must be hashable to be used as dictionary keys.
        flat
            How to structure the output dictionary's ID dimensions
            when multiple IDs are provided:
            - If `True`, the output dictionary will only have one ID dimension,
            with keys corresponding to ID tuples.
            - If `False`, the output dictionary will be nested,
            with the first ID values as first-dimension keys,
            the second ID values as second-dimension keys, and so on.

            When only one ID is provided, this parameter has no effect
            and the output will always have a single ID dimension
            with keys corresponding to the ID values.
        single_col
            How to structure the output dictionary's data dimension
            (i.e., the value of the inner-most ID dictionary)
            when there is only one data (non-ID) column in the DataFrame:
            - If "value", the output dictionary's data dimension will be
            the column value directly.
            - If "dict", the output dictionary's data dimension will be dictionaries
            with the data column name as key and the column value as value.

            When there are multiple data columns, this parameter has no effect
            and the output will always have dictionaries as the data dimension.
        single_row
            How to handle ID groups that correspond to a single row:
            - If "value", data values are returned directly.
            - If "list", data values are returned as single-item lists.
        multi_row
            How to handle ID groups that correspond to multiple rows:
            - If "list", data values are returned as lists.
            - If "first", only the first row's data values are returned.
            - If "last", only the last row's data values are returned.
        multi_row_warn
            If `True`, issue a warning when dropping rows,
            i.e., when ID groups correspond to multiple rows
            and `multi_row` is set to "first" or "last".

        Returns
        -------
        dict
            Dictionary representation of the DataFrame.

        Raises
        ------
        ValueError
        - If `ids` is empty.
        - If any of the specified ID columns are not found in the DataFrame.
        - If ID values are unhashable.
        - If the DataFrame has no data (non-ID) columns.
        """
        return dataframe_to_dict(
            self._df,
            ids=ids,
            flat=flat,
            single_col=single_col,
            single_row=single_row,
            multi_row=multi_row,
            multi_row_warn=multi_row_warn,
        )

    @abstractmethod
    def write(
        self,
        writer: Callable[[str], None],
        **kwargs,
    ) -> None:
        """Write the CIF data structure to the writer."""
        raise NotImplementedError("Subclasses must implement the write method.")

    @property
    def df(self) -> pl.DataFrame:
        """DataFrame representation of the CIF data structure."""
        return self._df

    def __str__(self) -> str:
        """String representation of the CIF data structure."""
        chunks = []
        self.write(chunks.append)
        return "".join(chunks)
