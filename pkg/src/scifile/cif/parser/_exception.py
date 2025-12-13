"""CIF parser exceptions and error handling."""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ._token import Token
from ._state import State

if TYPE_CHECKING:
    import re
    from ._parser import SeenCodeInfo


__all__ = [
    "CIFParsingError",
    "CIFParsingErrorType",
]


_EXPECTED_TOKENS = {
    State.IN_FILE: (Token.BLOCK_CODE,),
    State.JUST_IN_DATA: (Token.FRAME_CODE, Token.LOOP, Token.NAME),
    State.JUST_IN_SAVE: (Token.LOOP, Token.NAME),
    State.JUST_IN_LOOP: (Token.NAME,),
    State.IN_NAME: (Token.VALUE, Token.VALUE_QUOTED, Token.VALUE_FIELD),
    State.JUST_IN_SAVE_LOOP: (Token.NAME,),
    State.IN_SAVE_NAME: (Token.VALUE, Token.VALUE_QUOTED, Token.VALUE_FIELD),
    State.IN_LOOP_NAME: (Token.NAME, Token.VALUE, Token.VALUE_QUOTED, Token.VALUE_FIELD),
    State.IN_DATA: (Token.BLOCK_CODE, Token.FRAME_CODE, Token.LOOP, Token.NAME),
    State.IN_SAVE_LOOP_NAME: (Token.NAME, Token.VALUE, Token.VALUE_QUOTED, Token.VALUE_FIELD),
    State.IN_SAVE: (Token.FRAME_END, Token.LOOP, Token.NAME),
    State.IN_LOOP_VALUE: (
        Token.BLOCK_CODE,
        Token.FRAME_CODE,
        Token.LOOP,
        Token.NAME,
        Token.VALUE,
        Token.VALUE_QUOTED,
        Token.VALUE_FIELD,
    ),
    State.IN_SAVE_LOOP_VALUE: (
        Token.FRAME_END,
        Token.LOOP,
        Token.NAME,
        Token.VALUE,
        Token.VALUE_QUOTED,
        Token.VALUE_FIELD,
    ),
}
"""A mapping from each possible state to the expected tokens in that state.

This is only used for generating error messages
when unexpected tokens are encountered.
Otherwise, a more verbose mapping is defined in `MMCIFParser.__init__`, used by the parser
to validate and update its state.
"""


class CIFParsingErrorType(Enum):
    """Types of errors that may occur during parsing."""
    BLOCK_CODE_DUPLICATE = 1
    FRAME_CODE_DUPLICATE = 1
    BLOCK_CODE_EMPTY = 2
    DATA_NAME_EMPTY = 2
    TABLE_INCOMPLETE = 2
    TOKEN_BAD = 2
    TOKEN_RESERVED = 2
    TOKEN_UNEXPECTED = 2
    LOOP_NAMED = 3
    FILE_INCOMPLETE = 2


class CIFParsingError(Exception):
    def __init__(
        self,
        error_type: CIFParsingErrorType,
        *,
        state: State,
        token_idx: int,
        match: re.Match,
        token_type: Token,
        token_value: str,
        block_code: str | None,
        frame_code: str | None,
        frame_code_category: str | None,
        frame_code_keyword: str | None,
        data_name_category: str | None,
        data_name_keyword: str | None,
        data_value: str | None,
        seen_block_codes: dict[str, SeenCodeInfo],
        seen_frame_codes: dict[str, SeenCodeInfo],
    ):
        self.error_type = error_type

        self.state = state
        self.token_idx = token_idx
        self.token_start = match.start()
        self.token_end = match.end()
        self.match = match

        self.token_type = token_type
        self.token_value = token_value
        self.block_code = block_code
        self.frame_code = frame_code
        self.frame_code_category = frame_code_category
        self.frame_code_keyword = frame_code_keyword
        self.data_name_category = data_name_category
        self.data_name_keyword = data_name_keyword
        self.data_value = data_value
        self.seen_block_codes = seen_block_codes
        self.seen_frame_codes = seen_frame_codes
        error_handler = getattr(self, f"_{error_type.name.lower()}")
        self.error_msg, self.error_data = error_handler()
        super().__init__(self.error_msg)
        return

    def _block_code_duplicate(self) -> tuple[str, dict]:
        """Generate error message and data for duplicated block code error."""
        seen_info = self.seen_block_codes[self.block_code]
        error_data = {
            "block_code": self.block_code,
            "original": {
                "token_idx": seen_info.idx,
                "start": seen_info.start,
                "end": seen_info.end,
            },
            "duplicate": {
                "token_idx": self.token_idx,
                "start": self.token_start,
                "end": self.token_end,
            },
        }
        error_msg = (
            f"Duplicated block code: The block code 'data_{self.block_code}' "
            f"is already declared at token index {seen_info.idx} "
            f"(position {seen_info.start}-{seen_info.end} in the file), "
            f"but a second declaration was encountered at position "
            f"{self.token_start}-{self.token_end} of the file."
        )
        return error_msg, error_data

    def _frame_code_duplicate(self) -> tuple[str, dict]:
        """Generate error message and data for duplicated frame code error."""
        seen_info = self.seen_frame_codes[self.frame_code]
        error_data = {
            "frame_code": self.frame_code,
            "frame_code_category": self.frame_code_category,
            "frame_code_keyword": self.frame_code_keyword,
            "original": {
                "token_idx": seen_info.idx,
                "start": seen_info.start,
                "end": seen_info.end,
            },
            "duplicate": {
                "token_idx": self.token_idx,
                "start": self.token_start,
                "end": self.token_end,
            },
        }
        error_msg = (
            f"Duplicated frame code: The frame code 'save_{self.frame_code}' "
            f"is already declared at token index {seen_info.idx} "
            f"(position {seen_info.start}-{seen_info.end} in the file), "
            f"but a second declaration was encountered at position "
            f"{self.token_start}-{self.token_end} of the file."
        )
        return error_msg, error_data

    def _block_code_empty(self) -> tuple[str, dict]:
        """Generate error message and data for empty block code error."""
        error_data = {
            "token_idx": self.token_idx,
            "start": self.token_start,
            "end": self.token_end,
        }
        error_msg = (
            f"Empty block code: The block code at token index {self.token_idx} "
            f"(position {self.token_start}-{self.token_end} in the file) is empty."
        )
        return error_msg, error_data

    def _table_incomplete(self) -> tuple[str, dict]:
        """Generate error message and data for incomplete table error."""
        error_data = {
            "block_code": self.block_code,
            "frame_code": self.frame_code,
            "data_name_category": self.data_name_category,
            "data_name_keyword": self.data_name_keyword,
            "token_idx": self.token_idx,
            "start": self.token_start,
            "end": self.token_end,
        }
        error_msg = (
            f"Incomplete table: The table in block '{self.block_code}', "
            f"frame '{self.frame_code}', category '{self.data_name_category}' "
            f"and keyword '{self.data_name_keyword}' is incomplete. "
            f"The parser reached token index {self.token_idx} "
            f"(position {self.token_start}-{self.token_end} in the file) "
            f"before all expected data values were found."
        )
        return error_msg, error_data

    def _token_bad(self) -> tuple[str, dict]:
        """Generate error message and data for bad token error."""
        error_data = {
            "token_idx": self.token_idx,
            "start": self.token_start,
            "end": self.token_end,
            "token_value": self.token_value,
        }
        error_msg = (
            f"Bad token: The token at index {self.token_idx} "
            f"(position {self.token_start}-{self.token_end} in the file) "
            f"does not match any valid CIF token pattern: '{self.token_value}'."
        )
        return error_msg, error_data

    def _token_reserved(self) -> tuple[str, dict]:
        """Generate error message and data for reserved token error."""
        error_data = {
            "token_idx": self.token_idx,
            "start": self.token_start,
            "end": self.token_end,
            "token_type": self.token_type,
            "token_value": self.token_value,
        }
        error_msg = (
            f"Reserved token: The token at index {self.token_idx} "
            f"(position {self.token_start}-{self.token_end} in the file) "
            f"is a reserved STAR token of type '{self.token_type.name}': "
            f"'{self.token_value}'. Such tokens are not allowed in CIF files."
        )
        return error_msg, error_data

    def _token_unexpected(self) -> tuple[str, dict]:
        """Generate error message and data for unexpected token error."""
        expected_tokens = _EXPECTED_TOKENS.get(self.state, [])
        error_data = {
            "token_idx": self.token_idx,
            "start": self.token_start,
            "end": self.token_end,
            "token_type": self.token_type,
            "token_value": self.token_value,
            "expected_tokens": expected_tokens,
        }
        error_msg = (
            f"Unexpected token: The token at index {self.token_idx} "
            f"(position {self.token_start}-{self.token_end} in the file) "
            f"is of type '{self.token_type.name}': '{self.token_value}', "
            f"which is not expected in state {self.state}. "
            f"Expected token types are: "
            f"{', '.join(token.name for token in expected_tokens)}."
        )
        return error_msg, error_data

    def _loop_named(self) -> tuple[str, dict]:
        """Generate error message and data for named loop error."""
        error_data = {
            "token_idx": self.token_idx,
            "start": self.token_start,
            "end": self.token_end,
            "loop_name": self.token_value,
        }
        error_msg = (
            f"Named loop: The loop directive at token index {self.token_idx} "
            f"(position {self.token_start}-{self.token_end} in the file) "
            f"has a name '{self.token_value}'. "
            f"Loop directives must not have names."
        )
        return error_msg, error_data

    def _file_incomplete(self) -> tuple[str, dict]:
        """Generate error message and data for incomplete file error."""
        error_data = {
            "state": self.state,
            "expected_tokens": _EXPECTED_TOKENS.get(self.state, []),
        }
        error_msg = (
            f"Incomplete file: The end of the file was reached in state {self.state}, "
            f"while one of the following tokens was expected: "
            f"{', '.join(token.name for token in _EXPECTED_TOKENS.get(self.state, []))}."
        )
        return error_msg, error_data



    def _duplicate(self):
        return (
            f"Duplicated data item in data block '{self.block_code}': The data name "
            f"'_{self.data_name_category}.{self.data_name_keyword}' "
            f"(i.e. category '{self.data_name_category}', "
            f"keyword '{self.data_name_keyword}') "
            f"is already registered with a data value of "
            f"'{self._curr_data_category_dict[self.data_name_keyword]}', "
            f"but a second declaration with a value of '{self.data_value}' "
            f"was encountered at position {self.match.start()} of the file."
        )
