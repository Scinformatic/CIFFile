"""CIF data structure base class."""

from __future__ import annotations

from abc import abstractmethod, ABCMeta
from typing import Literal, Generic, TypeVar, Iterator, TypeAlias, Callable, Sequence, Self, TYPE_CHECKING, overload, Any

import polars as pl

from ciffile.typing import DataFrameLike
from ._util import dataframe_to_dict, validate_content_df, extract_categories, extract_files

if TYPE_CHECKING:
    from ._category import CIFDataCategory

ElementType = TypeVar("ElementType")
SingleIndexer: TypeAlias = str | int
MultiIndexer: TypeAlias = tuple[SingleIndexer, ...] | slice


class CIFStructure(Generic[ElementType], metaclass=ABCMeta):
    """CIF data structure base class.

    This is the base class for all CIF data structures,
    providing common functionality for accessing contained elements.
    """

    def __init__(
        self,
        *,
        code: str | None,
        container_type: Literal["file", "block", "frames", "frame", "category", "item"],
    ) -> None:
        self._code = code
        self._container_type = container_type
        self._codes: list[str] | None = None
        self._element_dict: dict[str, ElementType] | None = None
        return

    @property
    def code(self) -> str | None:
        """Containter code.

        The type of code returned depends on the current container type:
        - file: `None` (files do not have codes)
        - frames: `None` (this is a collection of save frames)
        - block/frame: block/frame code
        - category: data name category
        - item: data name keyword
        """
        return self._code

    @property
    def codes(self) -> list[str]:
        """Codes of the elements directly within this container.

        The type of codes returned depends on the current container type:
        - file: block codes
        - frames: frame codes
        - block/frame: data name categories
        - category: data name keywords
        - item: data value indices
        """
        if self._codes is None:
            self._codes = self._get_codes()
        return self._codes

    @property
    def container_type(self) -> Literal["file", "block", "frames", "frame", "category", "item"]:
        """Type of this CIF data container."""
        return self._container_type

    def get(self, indexer: SingleIndexer) -> ElementType:
        """Get an element directly within this container by its code or index.

        If the requested element does not exist,
        an empty element of the appropriate type is returned.

        Parameters
        ----------
        indexer
            A code or index number of the element to get.

        Returns
        -------
        An element directly within this container, or an empty element if not found.
        The type of element returned depends on the current container type:
        - file: data block
        - frames: save frame
        - block/frame: data category (data name category)
        - category: data item (data name keyword)
        - item: data value
        """
        code = self.codes[indexer] if isinstance(indexer, int) else indexer
        if code in self:
            return self[indexer]
        return self._get_empty_element()

    def __iter__(self) -> Iterator[ElementType]:
        """Iterate over elements directly within this container.

        Returns
        -------
        An iterator over the container elements.
        The type of elements yielded depends on the current container type:
        - file: data blocks
        - frames: save frames
        - block/frame: data name categories
        - category: data name keywords
        - item: data values
        """
        for code in self.codes:
            yield self[code]

    @overload
    def __getitem__(self, indexer: SingleIndexer) -> ElementType: ...
    @overload
    def __getitem__(self, indexer: MultiIndexer) -> list[ElementType]: ...
    def __getitem__(self, indexer: SingleIndexer | MultiIndexer) -> ElementType | list[ElementType]:
        """Get elements directly within this container by their codes or indices.

        Parameters
        ----------
        indexer
            A single code/index number to get one element,
            or a tuple of codes/index numbers or a slice to get multiple elements.

        Returns
        -------
        A single container element if a single code/index is provided,
        or a list of container elements if multiple codes/indices are provided.
        The type of elements returned depends on the current container type:
        - file: data blocks
        - block save frames: save frames
        - data block/save frame: data categories (data name categories)
        - data category: data items (data name keywords)
        - data item: data values
        """
        if isinstance(indexer, str | int):
            indexer = (indexer,)
            single = True
        else:
            single = False

        if isinstance(indexer, tuple):
            codes = [
                self.codes[idx]
                if isinstance(idx, int)
                else idx
                for idx in indexer
            ]
        elif isinstance(indexer, slice):
            codes = self.codes[indexer]
        else:
            raise TypeError(f"indexer must be str, int, tuple, or slice, got {type(indexer)}")

        containers = self._elements
        out = [containers[k] for k in codes]

        if single:
            return out[0]
        return out

    def __contains__(self, code: str) -> bool:
        """Check if an element with the given code exists directly within this container.

        Parameters
        ----------
        code
            The code of the element to check for.
            The type of expected code depends on the current container type:
            - file: checks for a data block with the given block code.
            - frames: checks for a save frame with the given frame code.
            - block/frame: checks for a data category with the given category code (data name category).
            - category: checks for a data item with the given item code (data name keyword).
            - item: checks for a data value with the given index number.
        """
        return code in self.codes

    def __len__(self) -> int:
        """Number of elements directly in this container.

        The type of elements counted depends on the current container type:
        - file: data blocks.
        - block save frames: save frames.
        - data block/save frame: data categories (data name categories).
        - data category: data items (data name keywords).
        - data item: data values
        """
        return len(self.codes)

    @property
    def _elements(self) -> dict[str, ElementType]:
        """Mapping from element codes to element objects for all elements directly within this container.

        The type of elements returned depends on the current container type:
        - file: data blocks.
        - block save frames: save frames.
        - data block/save frame: data categories (data name categories).
        - data category: data items (data name keywords).
        - data item: data values.
        """
        if self._element_dict is None:
            self._element_dict = self._get_elements()
        return self._element_dict

    def refresh(self) -> None:
        """Refresh cached element codes and element mapping.

        This forces re-generation of the element codes
        and the mapping from codes to element objects
        the next time they are accessed.
        """
        self._codes = None
        self._element_dict = None
        return

    @abstractmethod
    def _get_codes(self) -> list[str]:
        """Get codes of the data containers directly within this container.

        If the current container is
        - file: returns data block codes.
        - block save frames: returns save frame codes.
        - data block/save frame: returns data category codes (data name categories).
        - data category: returns data item codes (data name keywords).
        """
        ...

    @abstractmethod
    def _get_elements(self) -> dict[str, ElementType]:
        """Get all data containers directly within this container.

        If the current container is
        - file: returns all data blocks.
        - block save frames: returns all save frames.
        - data block/save frame: returns all data categories (data name categories).
        - data category: returns all data items (data name keywords).
        """
        ...

    @abstractmethod
    def _get_empty_element(self) -> ElementType:
        """Get an empty element of the appropriate type for this container.

        If the current container is
        - file: returns an empty data block.
        - frames: returns an empty save frame.
        - block/frame: returns an empty data category (data name category).
        - category: returns an empty data item (data name keyword).
        """
        ...


class CIFStructureWithItem(CIFStructure[ElementType]):
    """CIF data structure base class.

    This is the base class for CIF data structures that contain data items,
    i.e., all except `CIFDataItem`.
    """

    def __init__(
        self,
        *,
        content: DataFrameLike,
        variant: Literal["cif1", "mmcif"],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._df = content if isinstance(content, pl.DataFrame) else pl.DataFrame(content, strict=False)
        self._variant: Literal["cif1", "mmcif"] = variant

        str_header = None
        str_footer = None
        if self.container_type == "block":
            str_header = f"data_{self.code}\n"
        elif self.container_type == "frame":
            frame_code = (
                f"_{self.code}"
                if variant == "mmcif" and "." in self.code and not self.code.startswith("_") else
                self.code
            )
            str_header = f"save_{frame_code}\n"
            str_footer = "save_\n"

        self._str_header = str_header
        self._str_footer = str_footer
        return

    @property
    def df(self) -> pl.DataFrame:
        """DataFrame representation of the CIF data structure."""
        return self._df

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
            df_name=f"{self.code} ({self.container_type})",
        )

    @overload
    def write(
        self,
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
    ) -> None: ...
    def write(
        self,
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
        """Write this container in CIF format.

        Parameters
        ----------
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
            Whether to write data categories in table format
            even if they are lists (i.e., contain only a single row).
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
            is used to output the CIF content,
            and `None` is returned.

        Raises
        ------
        TypeError
            If the data values are of unsupported types.
        ValueError
            If any multiline string contains a line beginning with ';',
            which cannot be represented exactly as a CIF 1.1 text field.
        """
        if writer is None:
            chunks: list[str] = []
            writer = chunks.append
            writer_provided = False
        else:
            writer_provided = True

        self._write(
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

        if writer_provided:
            return None
        return "".join(chunks)

    def __str__(self) -> str:
        """String representation of the CIF data structure."""
        chunks = []
        self.write(chunks.append)
        return "".join(chunks)

    def __eq__(self, other: Any) -> bool:
        """Equality comparison for CIF data structures."""
        if not isinstance(other, CIFStructureWithItem):
            return False
        if self.container_type != other.container_type:
            return False
        if self.code != other.code:
            return False
        df_self = self.df
        df_other = other.df
        if df_self.schema != df_other.schema:
            return False
        cols = sorted(df_self.columns)
        df_self_sorted = df_self.select(cols).sort(cols)
        df_other_sorted = df_other.select(cols).sort(cols)
        return df_self_sorted.equals(df_other_sorted, null_equal=True)

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
    ):
        """Write method implementation.

        Sub-classes may override this method to customize writing behavior.
        """
        indent_contained = (indent + indent_inner) if self.container_type in ("block", "frame") else indent
        header = self._str_header
        spaces = ' ' * indent
        if header is not None:
            writer(f"{spaces}{header}")
        for container in self:
            container.write(
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
                indent=indent_contained,
                indent_inner=indent_inner,
                delimiter_preference=delimiter_preference,
            )
        if self._container_type == "block":
            self.frames.write(
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
                indent=indent_contained,
                indent_inner=indent_inner,
                delimiter_preference=delimiter_preference,
            )
        footer = self._str_footer
        if footer is not None:
            writer(f"{spaces}{footer}")
        return


class CIFStructureWithCategory(CIFStructureWithItem[ElementType]):
    """CIF file data structure base class."""

    def __init__(
        self,
        *,
        content: DataFrameLike,
        validate: bool,
        require_block: bool,
        require_frame: bool,
        col_name_block: str | None,
        col_name_frame: str | None,
        col_name_cat: str,
        col_name_key: str,
        col_name_values: str,
        allow_duplicate_rows: bool = False,
        **kwargs,
    ) -> None:
        if validate:
            content = validate_content_df(
                content,
                require_block=require_block,
                require_frame=require_frame,
                allow_duplicate_rows=allow_duplicate_rows,
                col_name_block=col_name_block,
                col_name_frame=col_name_frame,
                col_name_cat=col_name_cat,
                col_name_key=col_name_key,
                col_name_values=col_name_values,
            )

        super().__init__(content=content, **kwargs)
        if col_name_frame in self.df.columns:
            if self.df.select(pl.col(col_name_frame).is_null().all()).item():
                self._df = self.df.drop(col_name_frame)
                col_name_frame = None
                filetype = "data"
            else:
                filetype = "dict"
        else:
            col_name_frame = None
            filetype = "data"

        self._col_block = col_name_block
        self._col_frame = col_name_frame
        self._col_frame = col_name_frame
        self._col_cat = col_name_cat
        self._col_key = col_name_key
        self._col_values = col_name_values
        self._allow_duplicate_rows = allow_duplicate_rows

        self._filetype = filetype
        return

    @property
    def type(self) -> Literal["data", "dict"]:
        """Type of the CIF file.

        Either:
        - "data": Structure is not a save frame and contains no save frames.
        - "dict": Structure is a save frame or contains save frames.
        """
        return self._filetype

    def category(
        self,
        *category: str,
        col_name_block: str | None = "_block",
        col_name_frame: str | None = "_frame",
        drop_redundant: bool = False,
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
        from ._category import CIFDataCategory
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


class CIFStructureWithFrame(CIFStructureWithCategory[ElementType]):
    """CIF file data structure base class."""

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._parts: dict[Literal["data", "dict", "dict_cat", "dict_key"], pl.DataFrame] = {}
        return

    def new(
        self,
        *,
        code: str | None = None,
        content: DataFrameLike | None = None,
        variant: Literal["cif1", "mmcif"] | None = None,
        validate: bool | None = None,
        col_name_block: str | None = None,
        col_name_frame: str | None = None,
        col_name_cat: str | None = None,
        col_name_key: str | None = None,
        col_name_values: str | None = None,
        allow_duplicate_rows: bool | None = None,
    ) -> Self:
        """Create a new object of the same type as this CIF structure.

        Create a new object based on this one,
        but with some parameters modified.
        If an argument is `None`,
        the corresponding attribute of self is used.

        Parameters
        ----------
        code
            Code.
        content
            Content DataFrame.
        variant
            CIF variant.
        validate
            Whether to validate the content DataFrame.
            If `None`, validation is performed
            if `content` is provided.
        col_name_block
            Name of the column to use for block codes in the new object.
        col_name_frame
            Name of the column to use for frame codes in the new object.
        col_name_cat
            Name of the column to use for category codes in the new object.
        col_name_key
            Name of the column to use for key codes in the new object.
        col_name_values
            Name of the column to use for value codes in the new object.
        """
        return type(self)(
            code=code if code is not None else self._code,
            content=content if content is not None else pl.DataFrame(),
            variant=variant if variant is not None else self._variant,
            validate=validate if validate is not None else (content is not None),
            col_name_block=col_name_block if col_name_block is not None else self._col_block,
            col_name_frame=col_name_frame if col_name_frame is not None else self._col_frame,
            col_name_cat=col_name_cat if col_name_cat is not None else self._col_cat,
            col_name_key=col_name_key if col_name_key is not None else self._col_key,
            col_name_values=col_name_values if col_name_values is not None else self._col_values,
            allow_duplicate_rows=allow_duplicate_rows if allow_duplicate_rows is not None else self._allow_duplicate_rows,
        )

    def part(
        self,
        *part: Literal["data", "dict", "dict_cat", "dict_key"],
    ) -> Self | None | dict[str, Self | None]:
        """Isolate data/dictionary parts of the container.

        Parameters
        ----------
        *part
            Parts to extract; from:
            - "data": Data file,
              i.e., data items that are directly under data blocks
              (and not in any save frames).
            - "dict": Dictionary file,
              i.e., data items that are in save frames.
            - "dict_cat": Category dictionary file,
              i.e., data items that are in save frames without a frame code keyword
              (no period in the frame code).
            - "dict_key": Key dictionary file,
              i.e., data items that are in save frames with a frame code keyword
              (period in the frame code).

            If none provided, all parts found are extracted.

        Returns
        -------
        isolated_parts
            A single object like self if only one part is requested,
            or a dictionary of objects
            keyed by part name otherwise.
        """
        parts = set(part) if part else {"data", "dict", "dict_cat", "dict_key"}

        out = {}
        for p in parts:
            part_df = self._get_part(p)
            part_obj = self.new(
                content=part_df,
                validate=False,
            ) if not part_df.is_empty() else None
            out[p] = part_obj

        if len(parts) == 1:
            return out[next(iter(parts))]
        return out

    def _get_part(self, part: Literal["data", "dict", "dict_cat", "dict_key"]) -> pl.DataFrame:
        """Get data/dictionary part of the structure.

        Parameters
        ----------
        part
            Part to extract; from:
            - "data": Data items that are directly under the data block
            - "dict": Dictionary items that are directly under the data block
            - "dict_cat": Category dictionary items
            - "dict_key": Key dictionary items

        Returns
        -------
        pl.DataFrame
            Extracted part of the data block.
        """
        file_part = self._parts.get(part)
        if file_part is not None:
            return file_part

        self._parts = {
            "data": self._df,
            "dict": pl.DataFrame(),
            "dict_cat": pl.DataFrame(),
            "dict_key": pl.DataFrame(),
        } if self._col_frame is None else extract_files(
            df=self._df,
            col_name_frame=self._col_frame,
        )
        return self._parts[part]
