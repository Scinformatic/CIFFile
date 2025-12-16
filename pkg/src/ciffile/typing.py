"""CIFFile type-hint definitions.

This module defines type-hints used throughout the package.
"""

from pathlib import Path
from typing import TypeAlias

import polars._typing as plt


DataFrameLike: TypeAlias = plt.FrameInitTypes
"""A DataFrame-like input, compatible with Polars DataFrames."""


FileLike: TypeAlias = str | bytes | Path
"""A file-like input.

- If a `pathlib.Path` is provided, it is interpreted as the path to a file.
- If `bytes` are provided, they are interpreted as the content of the file.
- If a `str` is provided, it is interpreted as the content of the file.
"""

PathLike: TypeAlias = str | Path
"""A file path, either as a string or a `pathlib.Path` object."""


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
or a unique value (e.g., "1", "2", ...) for each table,
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
