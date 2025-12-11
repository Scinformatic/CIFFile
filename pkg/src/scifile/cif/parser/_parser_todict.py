"""CIF file parser to convert CIF files into flat/nested dictionaries."""


from typing import TypedDict
import itertools


from scifile.cif.exception import CIFParsingErrorType
from scifile.cif.parser._parser import CIFParser


class CIFFlatDict(TypedDict):
    """TypedDict for CIF file represented as a flat dictionary."""

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

        self._block_codes: list[str] = list()
        self._frame_code_categories: list[str | None] = list()
        self._frame_code_keywords: list[str | None] = list()
        self._data_name_categories: list[str] = list()
        self._data_name_keywords: list[str] = list()
        self._data_values: list[list[str]] = list()
        self._loop_id: list[int] = list()

        self._loop_value_lists: itertools.cycle = None
        self._loop_value_lists_idx: itertools.cycle = None

        self._curr_loop_id: int = 0
        self._curr_loop_columns: list[list[str]] = list()
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
