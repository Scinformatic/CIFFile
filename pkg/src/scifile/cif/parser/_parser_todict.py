"""CIF file parser to convert CIF files into flat/nested dictionaries."""


from typing import TypedDict
import itertools


from ._exception import CIFParsingErrorType
from ._parser import CIFParser


class CIFFlatDict(TypedDict):
    """Flat dictionary representation of a CIF file.

    Each key corresponds to a column in a table,
    where each row corresponds to a unique data item in the CIF file.

    Attributes
    ----------
    block_code
        List of block codes (i.e., data block names) for each data item.
        This is the top-level grouping in a CIF file,
        where each data item belongs to a specific data block.
    frame_code_category
        List of frame code categories (i.e., first part of save frame names) for each data item.
        This is the second-level grouping in a CIF file,
        where data items are either directly under a data block
        or belong to a specific save frame within a data block.
        For data items not in a save frame, this value is `None`.
        Note that this implementation breaks the frame code into two parts:
        - category: anything before the fist period (.)
        - keyword: anything after the first period (.)
    frame_code_keyword
        List of frame code keywords (i.e., second part of save frame names) for each data item.
        For data items not in a save frame, or frame codes without a period, this value is `None`.
        See `frame_code_category` for more details.
    data_name_category
        List of data name categories (i.e., first part of data item names) for each data item.
        Note that this implementation breaks the data name into two parts:
        - category: anything before the first period (.)
        - keyword: anything after the first period (.)
    data_name_keyword
        List of data name keywords (i.e., second part of data item names) for each data item.
        For data names without a period, this value is `None`.
        See `data_name_category` for more details.
    data_values
        List of data values for each data item.
        Each data value is represented as a list of strings.
        For single data items, this list will contain a single string.
        For tabular (looped) data items, this list will contain multiple strings,
        corresponding to row values for that data item column in the table.
    loop_id
        List of loop IDs for each data item.
        A loop ID of `0` indicates that the data item is a single item
        (i.e., not part of a looped table).
        A positive loop ID (1, 2, ...) indicates that the data item
        is part of a looped table, with the same loop ID
        shared among all data items in that table.
    """

    block_code: list[str]
    frame_code_category: list[str | None]
    frame_code_keyword: list[str | None]
    data_name_category: list[str]
    data_name_keyword: list[str]
    data_values: list[list[str]]
    loop_id: list[int]


class CIFToDictParser(CIFParser):
    """CIF file parser."""

    def __init__(self):
        super().__init__()

        self._block_codes: list[str] = []
        self._frame_code_categories: list[str | None] = []
        self._frame_code_keywords: list[str | None] = []
        self._data_name_categories: list[str | None] = []
        self._data_name_keywords: list[str | None] = []
        self._data_values: list[list[str]] = []
        self._loop_id: list[int] = []

        self._loop_value_lists: itertools.cycle = None
        self._loop_value_lists_idx: itertools.cycle = None

        self._curr_loop_id: int = 0
        self._curr_loop_columns: list[list[str]] = []
        return

    # Implementation of abstract methods from CIFParser

    def _return_data(self) -> CIFFlatDict:
        flat_dict = CIFFlatDict(
            block_code=self._block_codes,
            frame_code_category=self._frame_code_categories,
            frame_code_keyword=self._frame_code_keywords,
            data_name_category=self._data_name_categories,
            data_name_keyword=self._data_name_keywords,
            data_values=self._data_values,
            loop_id=self._loop_id,
        )
        return flat_dict

    def _add_data_item(self):
        self._add_data(data_value=[self.curr_data_value], loop_id=0)
        return

    def _initialize_loop(self):
        self._curr_loop_id += 1
        self._curr_loop_columns = list()
        self._add_loop_keyword()
        return

    def _add_loop_keyword(self):
        new_column = []
        self._curr_loop_columns.append(new_column)
        self._add_data(data_value=new_column, loop_id=self._curr_loop_id)
        return

    def _register_and_fill_loop(self):
        self._register_loop()
        self._fill_loop_value()
        return

    def _fill_loop_value(self):
        next(self._loop_value_lists).append(self.curr_data_value)
        next(self._loop_value_lists_idx)
        return

    def _finalize_loop(self):
        if next(self._loop_value_lists_idx) != 0:
            self._register_error(CIFParsingErrorType.TABLE_INCOMPLETE)
        return

    # Private Methods
    # ===============

    def _add_data(self, data_value: str | list, loop_id: int):
        self._block_codes.append(self.curr_block_code)
        self._frame_code_categories.append(self.curr_frame_code_category)
        self._frame_code_keywords.append(self.curr_frame_code_keyword)
        self._data_name_categories.append(self.curr_data_name_category)
        self._data_name_keywords.append(self.curr_data_name_keyword)
        self._data_values.append(data_value)
        self._loop_id.append(loop_id)
        return

    def _register_loop(self):
        self._loop_value_lists = itertools.cycle(self._curr_loop_columns)
        self._loop_value_lists_idx = itertools.cycle(range(len(self._curr_loop_columns)))
        return
