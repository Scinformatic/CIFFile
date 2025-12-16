from typing import Literal, Callable, Sequence

import polars as pl

from ciffile.writer import category as write_category

from ._skel import CIFSkeleton


class CIFDataCategory(CIFSkeleton):
    """CIF file data category."""

    def __init__(
        self,
        code: str,
        content: pl.DataFrame,
        *,
        variant: Literal["cif1", "mmcif"],
        col_name_block: str | None = None,
        col_name_frame: str | None = None,
    ):
        super().__init__(
            content=content,
            variant=variant,
        )
        self._code = code
        self._col_block = col_name_block
        self._col_frame = col_name_frame
        return

    @property
    def code(self) -> str:
        """Data category name."""
        return self._code

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
        """Write this data category in CIF format.

        Parameters
        ----------
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
        None
            Uses the provided `writer` callable to output the CIF data category.

        Raises
        ------
        TypeError
            If the input DataFrame contains unsupported dtypes.
        ValueError
            If any multiline string contains a line beginning with ';',
            which cannot be represented exactly as a CIF 1.1 text field.
        """
        exclude_columns = [col for col in (self._col_block, self._col_frame) if col is not None]
        df = self.df.select(pl.exclude(exclude_columns))
        if self._variant == "mmcif":
            # Set column names to full data names
            df = df.select(pl.all().name.prefix(f"_{self._code}."))
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

    def __repr__(self) -> str:
        return f"CIFDataCategory(name={self._code!r}, shape={self._df.shape})"
