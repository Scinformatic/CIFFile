"""CIF parser exceptions and error handling."""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ._token import Token
from ._state import State

if TYPE_CHECKING:
    from ._parser import CIFParser


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
    DUPLICATE_BLOCK_CODE = 1
    DUPLICATE_FRAME_CODE = 1
    DUPLICATE = 2
    TABLE_INCOMPLETE = 2
    BLOCK_CODE_EMPTY = 2
    BAD_TOKEN = 2
    RESERVED_TOKEN = 2
    UNEXPECTED_TOKEN = 2
    LOOP_HAS_NAME = 3
    INCOMPLETE_FILE = 2


class CIFParsingError(Exception):
    def __init__(self, parser: CIFParser, error_type: CIFParsingErrorType):
        self.error_type = error_type

        self.state = parser.curr_state
        self.token_idx = parser.curr_token_idx
        self.token_start = parser.curr_match.start()
        self.token_end = parser.curr_match.end()
        self.match = parser.curr_match
        self.token_type = parser.curr_token_type
        self.token_value = parser.curr_token_value
        self.block_code = parser.curr_block_code
        self.frame_code = parser.curr_frame_code
        self.frame_code_category = parser.curr_frame_code_category
        self.frame_code_keyword = parser.curr_frame_code_keyword
        self.data_name_category = parser.curr_data_name_category
        self.data_name_keyword = parser.curr_data_name_keyword
        self.data_value = parser.curr_data_value
        self.seen_block_codes = parser.seen_block_codes_in_file.copy()
        self.seen_frame_codes = parser.seen_frame_codes_in_block.copy()

        error_handler = getattr(self, f"_{error_type.name.lower()}")
        self.error_msg, self.error_data = error_handler()
        super().__init__(self.error_msg)
        return

    def _duplicate_block_code(self) -> tuple[str, dict]:
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
            f"Duplicated block code: The data block code 'data_{self.block_code}' "
            f"is already declared at token index {seen_info.idx} "
            f"(position {seen_info.start}-{seen_info.end} in the file), "
            f"but a second declaration was encountered at position "
            f"{self.token_start}-{self.token_end} of the file."
        )
        return error_msg, error_data

    def _duplicate_frame_code(self) -> tuple[str, dict]:
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

    def _table_incomplete(self):
        return

    def _bad_token(self):
        return f"Bad token: got {self.token_value} at position {self.match.span()}"

    def _unexpected_token(self):
        return (
            f"Token out of order: parser is in state {self.curr_state} "
            f"and expects a token from {_EXPECTED_TOKENS[self.curr_state]}, "
            f"but received a {self.curr_token_type}."
        )
