"""CIF parser exceptions and error handling."""

from __future__ import annotations

from enum import Enum, auto
from typing import TYPE_CHECKING

from ._token import Token
from ._state import State

if TYPE_CHECKING:
    import re
    from ._parser import SeenCodeInfo


__all__ = [
    "CIFFileParseError",
    "CIFFileParseErrorType",
]


class CIFFileParseErrorType(Enum):
    """Types of errors that may occur during parsing."""
    BLOCK_CODE_DUPLICATE = auto()
    FRAME_CODE_DUPLICATE = auto()
    BLOCK_CODE_EMPTY = auto()
    FRAME_CODE_EMPTY = auto()
    DATA_NAME_EMPTY = auto()
    DATA_NAME_DUPLICATE = auto()
    DATA_NAME_NOT_MMCIF = auto()
    TABLE_INCOMPLETE = auto()
    TABLE_MULTICAT = auto()
    TABLE_CAT_DUPLICATE = auto()
    TABLE_CAT_REPEATED = auto()
    TOKEN_BAD = auto()
    TOKEN_RESERVED = auto()
    TOKEN_UNEXPECTED = auto()
    LOOP_NAMED = auto()
    FILE_INCOMPLETE = auto()


class CIFFileParseError(Exception):
    def __init__(
        self,
        error_type: CIFFileParseErrorType,
        *,
        state: State,
        token_idx: int,
        match: re.Match | None,
        token_type: Token,
        token_value: str,
        block_code: str | None,
        frame_code: str | None,
        data_category: str | None,
        data_keyword: str | None,
        data_name: str | None,
        data_value: str | None,
        seen_block_codes: dict[str, SeenCodeInfo],
        seen_frame_codes: dict[str, SeenCodeInfo],
        seen_data_names_in_block: dict[str, SeenCodeInfo],
        seen_data_names_in_frame: dict[str, SeenCodeInfo],
        seen_data_categories_in_block: dict[str, SeenCodeInfo] | None = None,
        seen_data_categories_in_frame: dict[str, SeenCodeInfo] | None = None,
        seen_table_categories_in_block: dict[str, SeenCodeInfo] | None = None,
        seen_table_categories_in_frame: dict[str, SeenCodeInfo] | None = None,
        expected_tokens: list[Token],
    ):
        self.error_type = error_type

        self.state = state
        self.token_idx = token_idx
        self.token_start = match.start() if match is not None else -1
        self.token_end = match.end() if match is not None else -1
        self.match = match

        self.token_type = token_type
        self.token_value = token_value
        self.block_code = block_code
        self.frame_code = frame_code
        self.data_category = data_category
        self.data_keyword = data_keyword
        self.data_name = data_name
        self.data_value = data_value
        self.seen_block_codes = seen_block_codes
        self.seen_frame_codes = seen_frame_codes
        self.seen_data_names_in_block = seen_data_names_in_block
        self.seen_data_names_in_frame = seen_data_names_in_frame
        self.seen_data_categories_in_block = seen_data_categories_in_block
        self.seen_data_categories_in_frame = seen_data_categories_in_frame
        self.seen_table_categories_in_block = seen_table_categories_in_block
        self.seen_table_categories_in_frame = seen_table_categories_in_frame

        self.expected_tokens = expected_tokens or []
        self.seen: SeenCodeInfo | None = None

        self.address_index = (
            f"at token index {self.token_idx} "
            f"(character index range {self.token_start}-{self.token_end})"
        )
        if self.block_code is None:
            self.address_path = "in file"
        else:
            self.address_path = f"in data block 'data_{self.block_code}'"
            if self.frame_code is not None:
                self.address_path += f", save frame 'save_{self.frame_code}'"

        error_handler = getattr(self, f"_msg_{error_type.name.lower()}")
        self.message = error_handler()
        super().__init__(self.message)
        return

    def _msg_block_code_duplicate(self) -> str:
        """Generate error message and data for duplicated block code error."""
        self.seen = seen = self.seen_block_codes[self.block_code]
        error_msg = (
            "Duplicated block code: "
            f"The block code 'data_{self.block_code}' {self.address_index} "
            f"is already declared at token index {seen.idx} "
            f"(character index range {seen.start}-{seen.end})."
        )
        return error_msg

    def _msg_frame_code_duplicate(self) -> str:
        """Generate error message and data for duplicated frame code error."""
        self.seen = seen = self.seen_frame_codes[self.frame_code]
        error_msg = (
            "Duplicated frame code: "
            f"The frame code 'save_{self.frame_code}' "
            f"in data block 'data_{self.block_code}' {self.address_index} "
            f"is already declared at token index {seen.idx} "
            f"(character index range {seen.start}-{seen.end})."
        )
        return error_msg

    def _msg_block_code_empty(self) -> str:
        """Generate error message and data for empty block code error."""
        error_msg = (
            "Empty block code: "
            f"The block code {self.address_index} is empty."
        )
        return error_msg

    def _msg_frame_code_empty(self) -> str:
        """Generate error message and data for empty frame code error."""
        error_msg = (
            "Empty frame code: "
            f"The frame code in data block 'data_{self.block_code}' "
            f"{self.address_index} is empty."
        )
        return error_msg

    def _msg_data_name_empty(self) -> str:
        """Generate error message and data for empty data name error."""
        error_msg = (
            "Empty data name: "
            f"The data name {self.address_path}, {self.address_index} is empty."
        )
        return error_msg

    def _msg_data_name_duplicate(self) -> str:
        """Generate error message and data for duplicated data name error."""
        seen_codes = (
            self.seen_data_names_in_frame
            if self.frame_code is not None
            else self.seen_data_names_in_block
        )
        seen_info = seen_codes[self.data_name]
        error_msg = (
            "Duplicated data name: "
            f"The data name '_{self.data_name}' {self.address_path}, {self.address_index} "
            f"is already declared at token index {seen_info.idx} "
            f"(character index range {seen_info.start}-{seen_info.end})."
        )
        return error_msg

    def _msg_data_name_not_mmcif(self) -> str:
        """Generate error message and data for non-mmcif data name error."""
        error_msg = (
            "Non-mmcif data name: "
            f"The data name '_{self.data_name}' {self.address_path}, {self.address_index} "
            f"is not valid in mmCIF format. "
            f"mmCIF data names must contain exactly one period ('.') character."
        )
        return error_msg

    def _msg_table_cat_duplicate(self) -> str:
        """Generate error message and data for duplicated category in table error."""
        seen_codes = (
            self.seen_data_categories_in_frame
            if self.frame_code is not None
            else self.seen_data_categories_in_block
        )
        seen_info = seen_codes[self.data_category]
        error_msg = (
            "Duplicated category in table: "
            f"The table data category '{self.data_category}' {self.address_path}, {self.address_index} "
            f"is already declared earlier at token index {seen_info.idx} "
            f"(character index range {seen_info.start}-{seen_info.end}). "
            f"Each table's data category must be unique within its data block or save frame."
        )
        return error_msg

    def _msg_table_cat_repeated(self) -> str:
        """Generate error message and data for repeated category in table error."""
        seen_codes = (
            self.seen_table_categories_in_frame
            if self.frame_code is not None
            else self.seen_table_categories_in_block
        )
        seen_info = seen_codes[self.data_category]
        error_msg = (
            "Repeated category in table: "
            f"The data category '{self.data_category}' {self.address_path}, {self.address_index} "
            f"is already declared in an earlier table with token index {seen_info.idx} "
            f"(character index range {seen_info.start}-{seen_info.end}). "
            f"Each table's data category must be unique within its data block or save frame."
        )
        return error_msg

    def _msg_table_multicat(self) -> str:
        """Generate error message and data for multi-category table error."""
        error_msg = (
            "Multi-category table: "
            f"The table {self.address_path}, {self.address_index} "
            f"contains data items from multiple categories. "
            f"All data items in a table must belong to the same category, "
            f"but the current data category '{self.data_category}' is not the same as the earlier one."
        )
        return error_msg

    def _msg_table_incomplete(self) -> str:
        """Generate error message and data for incomplete table error."""
        error_msg = (
            "Incomplete table: "
            f"The table {self.address_path} is incomplete. "
            f"The parser reached {self.address_index} "
            f"before all expected data values were found."
        )
        return error_msg

    def _msg_token_bad(self) -> str:
        """Generate error message and data for bad token error."""
        error_msg = (
            "Bad token: "
            f"The token {self.address_path}, {self.address_index} "
            f"does not match any valid CIF token pattern: '{self.token_value}'."
        )
        return error_msg

    def _msg_token_reserved(self) -> str:
        """Generate error message and data for reserved token error."""
        error_msg = (
            "Reserved token: "
            f"The token {self.address_path}, {self.address_index} "
            f"is a reserved STAR token of type '{self.token_type.name}': "
            f"'{self.token_value}'. Such tokens are not allowed in CIF files."
        )
        return error_msg

    def _msg_token_unexpected(self) -> str:
        """Generate error message and data for unexpected token error."""
        expected_tokens = self.expected_tokens
        error_msg = (
            "Unexpected token: "
            f"The token {self.address_path}, {self.address_index} "
            f"is of type '{self.token_type.name}': '{self.token_value}', "
            f"which is not expected in state {self.state}. "
            f"Expected token types are: "
            f"{', '.join(token.name for token in expected_tokens)}."
        )
        return error_msg

    def _msg_loop_named(self) -> str:
        """Generate error message and data for named loop error."""
        error_msg = (
            "Named loop: "
            f"The loop directive {self.address_path}, {self.address_index} "
            f"has a name '{self.token_value}'. "
            f"Loop directives must not have names."
        )
        return error_msg

    def _msg_file_incomplete(self) -> str:
        """Generate error message and data for incomplete file error."""
        error_msg = (
            "Incomplete file: "
            f"The end of the file was reached in state {self.state}, "
            f"while one of the following tokens was expected: "
            f"{', '.join(token.name for token in self.expected_tokens)}."
        )
        return error_msg
