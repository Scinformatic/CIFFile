"""CIFFile type-hint definitions.

This module defines type-hints used throughout the package.
"""

from typing import TypeAlias, Any

from fileex.typing import FileLike, PathLike
import polars as pl
import polars._typing as plt


__all__ = [
    "DataFrameLike",
    "BlockCode",
    "FrameCode",
    "DataCategory",
    "DataKeyword",
    "DataValues",
    "FileLike",
    "PathLike",
]


DataFrameLike: TypeAlias = plt.FrameInitTypes
"""A DataFrame-like input, compatible with Polars DataFrames."""

DataTypeLike: TypeAlias = plt.PolarsDataType | plt.PythonDataType
"""A data type compatible with Polars data types."""


BlockCode: TypeAlias = str
"""block code (i.e., data block name) of a data item.

This is the top-level grouping in a CIF file,
where each data item belongs to a specific data block.
"""

FrameCode: TypeAlias = str | None
"""frame code (i.e., save frame name) of a data item.

This is the second-level grouping in a CIF file,
where data items are either directly under a data block
or belong to a specific save frame within a data block.
For data items not in a save frame, this value is `None`.
"""

DataCategory: TypeAlias = str | None
"""data category of a data item.

For mmCIF files, this corresponds to
the part before the period in the data name.
For CIF files, this must be `None` for single data items
(i.e., not part of a loop/table),
and a unique value (e.g., "1", "2", ...) for each table,
shared among all data items in that table.
"""

DataKeyword: TypeAlias = str
"""data keyword of a data item.

For mmCIF files, this corresponds to
the part after the period in the data name.
For CIF files, this is the data name itself.
"""

DataValues: TypeAlias = list[str]
"""data values of a data item.

Each data value is represented as a list of strings.
For single data items, this list will contain a single string.
For tabular (looped) data items, this list will contain multiple strings,
corresponding to row values for that data item column in the table.
"""
