from __future__ import annotations

from typing import Mapping, Literal, TypedDict, overload, TYPE_CHECKING, Sequence, Callable

import polars as pl

from ._category import write as category
from ciffile.typing import DataFrameLike

if TYPE_CHECKING:
    from ciffile.structure import CIFDataCategory

__all__ = [
    "category",
    "write",
]


@overload
def write(
    file: Mapping[
        str,
        Mapping[
            str | None,
            list[CIFDataCategory | DataFrameLike]
        ]
    ],
    writer: None = None,
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
) -> str: ...

@overload
def write(
    file: Mapping[
        str,
        Mapping[
            str | None,
            list[CIFDataCategory | DataFrameLike]
        ]
    ],
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
) -> None: ...

def write(
    file: Mapping[
        str,
        Mapping[
            str | None,
            list[CIFDataCategory | DataFrameLike]
        ]
    ],
    writer: Callable[[str], None] | None = None,
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
) -> str | None:
    """Write a CIF file data structure in CIF format.

    Parameters
    ----------
    file
        CIF file data structure to write.
        This must be a mapping of data block codes to mappings of
        save frame codes to lists of data categories,
        where each data category is either a `CIFDataCategory` instance,
        or a Polars `DataFrame` (or any data convertible to it).
        A `None` save frame code indicates data categories directly
        in the data block (no save frame).
    writer
        A callable that takes a string and writes it to the desired output.
        This could be a file write method or any other string-consuming function.
        For example, you can create a list and pass its `append` method
        to collect the output chunks into the list.
        The whole CIF content can then be obtained by joining the list elements,
        i.e., `''.join(output_list)`.
        If `None` (default), the function will return
        the entire CIF content as a single string.
    bool_true
        Symbol to use for boolean `True` values.
    bool_false
        Symbol to use for boolean `False` values.
    null_str
        Symbol to use for null values in string columns.
    null_float
        Symbol to use for null values in floating-point columns.
    null_int
        Symbol to use for null values in integer columns.
    null_bool
        Symbol to use for null values in boolean columns.
    empty_str
        Symbol to use for empty strings in string columns.
    nan_float
        Symbol to use for NaN values in floating-point columns.
    always_table
        Whether to write the data category in table format
        even if it is a list (i.e., contains only a single row).
        When `False` (default),
        single-row categories are written as lists.
    list_style
        Style to use when writing a list (single-row category).
        Options:
        - "horizontal": All data items on a single line, separated by spaces:
          ```
          _name1 value1 _long_name2 value2 _name3 value3 ...
          ```
        - "tabular": Each data item on its own line, aligned in a table:
          ```
          _name1       value1
          _long_name2  value2
          _name3       value3
          ...
          ```
        - "vertical": Each token on its own line:
          ```
          _name1
          value1
          _long_name2
          value2
          _name3
          value3
          ...
          ```
    table_style
        Style to use when writing a table (multi-row category).
        Options:
        - "horizontal": All tokens on a single line, separated by spaces:
          ```
          loop_ _name1 _long_name2 _name3 value1_1 value2_1 value3_1 value1_2 value2_2 value3_2 ...
          ```
        - "tabular-horizontal": Each row (including headers) on its own line,
          aligned in a table:
          ```
          loop_
          _name1    _long_name2  _name3
          value1_1  value2_1     value3_1
          value1_2  value2_2     value3_2
          ...
          ```
        - "tabular-vertical": Vertical header with each row on its own line,
          aligned in a table:
          ```
          loop_
          _name1
          _long_name2
          _name3
          value1_1  value2_1  value3_1
          value1_2  value2_2  value3_2
          ...
          ```
        - "vertical": Each token on its own line:
          ```
          loop_
          _name1
          _long_name2
          _name3
          value1_1
          value2_1
          value3_1
          value1_2
          value2_2
          value3_2
          ...
          ```
    space_items
        Number of spaces to use
        between name-value pairs in horizontal lists:
        ```
        _name1 value1<space_items>_long_name2 value2 ...
        ```
    min_space_columns
        Minimum number of spaces to use
        between columns in tabular formats:
        ```
        _name1  <min_space_columns>_long_name2<min_space_columns>_name3
        value1_1<min_space_columns>value2_1   <min_space_columns>value3_1
        ...
        ```
    indent
        Number of spaces to indent each line
        of the overall data category output:
        ```
        <indent>loop_
        <indent>_name1 _name2 ...
        <indent>value1_1 value2_1 ...
        ```
    indent_inner
        Number of spaces to indent each line
        inside loop constructs (table body):
        ```
        loop_
        <indent_inner>_name1 _name2 ...
        <indent_inner>value1_1 value2_1 ...
        ```
    delimiter_preference
        Order of preference for string delimiters/quotations,
        from most to least preferred.

    Returns
    -------
    cif_content
        If `writer` is not provided (i.e., is `None`),
        the entire CIF content is returned as a single string.
        Otherwise, the provided `writer` callable
        is used to output the CIF file content,
        and `None` is returned.

    Raises
    ------
    TypeError
        If the input contains unsupported dtypes,
        such as nested data structures.
    ValueError
        If any multiline string contains a line beginning with ';',
        which cannot be represented exactly as a CIF 1.1 text field.
    """
    from ciffile.structure import CIFDataCategory

    if writer is None:
        chunks: list[str] = []
        writer = chunks.append
        writer_provided = False
    else:
        writer_provided = True

    common_args = {
        "writer": writer,
        "bool_true": bool_true,
        "bool_false": bool_false,
        "null_str": null_str,
        "null_float": null_float,
        "null_int": null_int,
        "null_bool": null_bool,
        "empty_str": empty_str,
        "nan_float": nan_float,
        "always_table": always_table,
        "list_style": list_style,
        "table_style": table_style,
        "space_items": space_items,
        "min_space_columns": min_space_columns,
        "delimiter_preference": delimiter_preference,
    }
    for block_code, block_content in file.items():
        block_header = f"{' ' * indent}data_{block_code}\n"
        writer(block_header)
        for frame_code, frame_content in block_content.items():
            frame_spaces = ' ' * (indent + indent_inner)
            if frame_code is None:
                category_indent = indent + indent_inner
            else:
                frame_header = f"{frame_spaces}save_{frame_code}\n"
                writer(frame_header)
                category_indent = indent + 2 * indent_inner
            for cat in frame_content:
                if isinstance(cat, CIFDataCategory):
                    cat._write(
                        **common_args,
                        indent=category_indent,
                        indent_inner=indent_inner,
                    )
                else:
                    df = cat if isinstance(cat, pl.DataFrame) else pl.DataFrame(cat)
                    category(
                        df,
                        **common_args,
                        indent=category_indent,
                        indent_inner=indent_inner,
                    )
            if frame_code is not None:
                frame_footer = f"{frame_spaces}save_\n"
                writer(frame_footer)
    if writer_provided:
        return None
    return "".join(chunks)
