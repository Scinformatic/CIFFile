
import itertools

import polars as pl

from scifile.cif.exception import CIFParsingErrorType
from scifile.cif.validator import CIFFileValidator
from scifile.cif.parser._parser import CIFParser


class CIFToDictParser(CIFParser):
    """CIF file parser."""

    def __init__(self):
        super().__init__()

        self._cif_dict_horizontal: dict = dict()

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

    def _finalize(self):
        super()._finalize()

        df = pl.DataFrame(
            dict(
                block_code=self._block_codes,
                frame_code_category=self._frame_code_categories,
                frame_code_keyword=self._frame_code_keywords,
                data_name_category=self._data_name_categories,
                data_name_keyword=self._data_name_keywords,
                data_value=self._data_values,
                loop_id=self._loop_id,
            ),
            dict(
                block_code=pl.Utf8,
                frame_code_category=pl.Utf8,
                frame_code_keyword=pl.Utf8,
                data_name_category=pl.Utf8,
                data_name_keyword=pl.Utf8,
                data_value=pl.List(pl.Utf8),
                loop_id=pl.UInt32,
            ),
        )
        return CIFFileValidator(df=df, errors=self.errors)

    def _add_data_item(self):
        # data_value_list = self._curr_data_keyword_list
        # if len(data_value_list) != 0:
        #     self._raise_or_warn(CIFParsingErrorType.DUPLICATE)
        # data_value_list.append(self.curr_data_value)
        self._add_data(data_value=[self.curr_data_value], loop_id=0)
        return

    def _initialize_loop(self):
        self._curr_loop_id += 1
        self._curr_loop_columns = list()
        self._add_loop_keyword()
        return

    def _add_loop_keyword(self):
        # column = self._curr_data_keyword_list
        # if len(column) != 0:
        #     self._raise_or_warn(CIFParsingErrorType.DUPLICATE)
        new_column = list()
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

    # Private Properties
    # ==================

    @property
    def _curr_data_block_dict(self) -> dict:
        return self._cif_dict_horizontal.setdefault(self.curr_block_code, dict())

    @property
    def _curr_save_frame_category_dict(self) -> dict:
        return self._curr_data_block_dict.setdefault(self.curr_frame_code_category, dict())

    @property
    def _curr_save_frame_keyword_dict(self) -> dict:
        return self._curr_save_frame_category_dict.setdefault(self.curr_frame_code_keyword, dict())

    @property
    def _curr_data_category_dict(self) -> dict:
        return self._curr_save_frame_keyword_dict.setdefault(self.curr_data_name_category, dict())

    # @property
    # def _curr_data_keyword_list(self) -> list:
    #     return self._curr_data_category_dict.setdefault(self.curr_data_name_keyword, list())

    # Private Methods
    # ===============

    def _add_data(self, data_value: str | list, loop_id: int):
        self._curr_data_category_dict[self.curr_data_name_keyword] = data_value

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
