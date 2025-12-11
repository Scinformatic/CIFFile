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
    State.IN_FILE: (Token.DATA,),
    State.JUST_IN_DATA: (Token.SAVE, Token.LOOP, Token.NAME),
    State.JUST_IN_SAVE: (Token.LOOP, Token.NAME),
    State.JUST_IN_LOOP: (Token.NAME,),
    State.IN_NAME: (Token.VALUE, Token.VALUE_QUOTED, Token.VALUE_FIELD),
    State.JUST_IN_SAVE_LOOP: (Token.NAME,),
    State.IN_SAVE_NAME: (Token.VALUE, Token.VALUE_QUOTED, Token.VALUE_FIELD),
    State.IN_LOOP_NAME: (Token.NAME, Token.VALUE, Token.VALUE_QUOTED, Token.VALUE_FIELD),
    State.IN_DATA: (Token.DATA, Token.SAVE, Token.LOOP, Token.NAME),
    State.IN_SAVE_LOOP_NAME: (Token.NAME, Token.VALUE, Token.VALUE_QUOTED, Token.VALUE_FIELD),
    State.IN_SAVE: (Token.SAVE_END, Token.LOOP, Token.NAME),
    State.IN_LOOP_VALUE: (
        Token.DATA,
        Token.SAVE,
        Token.LOOP,
        Token.NAME,
        Token.VALUE,
        Token.VALUE_QUOTED,
        Token.VALUE_FIELD,
    ),
    State.IN_SAVE_LOOP_VALUE: (
        Token.SAVE_END,
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

    DUPLICATE = 2
    TABLE_INCOMPLETE = 2
    BLOCK_CODE_EMPTY = 2
    BAD_TOKEN = 2
    RESERVED_TOKEN = 2
    UNEXPECTED_TOKEN = 2
    LOOP_HAS_NAME = 3
    INCOMPLETE_FILE = 2


class CIFParsingError(Exception):
    def __init__(self, parser_instance: CIFParser, error_type: CIFParsingErrorType):
        self.state = parser_instance.curr_state
        self.match = parser_instance.curr_match
        self.token_type = parser_instance.curr_token_type
        self.token_value = parser_instance.curr_token_value
        self.block_code = parser_instance.curr_block_code
        self.frame_code_category = parser_instance.curr_frame_code_category
        self.frame_code_keyword = parser_instance.curr_frame_code_keyword
        self.data_name_category = parser_instance.curr_data_name_category
        self.data_name_keyword = parser_instance.curr_data_name_keyword
        self.data_value = parser_instance.curr_data_value

        self.parser_instance = parser_instance
        self.error_type = error_type
        self.error_msg = self.error_message(parser_instance, error_type)
        super().__init__(self.error_msg)
        return

    @staticmethod
    def error_message(parser_instance: CIFParser, error_type: CIFParsingErrorType):
        return getattr(CIFParsingError, f"_{error_type.name.lower()}")(parser_instance)

    @staticmethod
    def _duplicate(parser: CIFParser):
        return (
            f"Duplicated data item in data block '{parser.curr_block_code}': The data name "
            f"'_{parser.curr_data_name_category}.{parser.curr_data_name_keyword}' "
            f"(i.e. category '{parser.curr_data_name_category}', "
            f"keyword '{parser.curr_data_name_keyword}') "
            f"is already registered with a data value of "
            f"'{parser._curr_data_category_dict[parser.curr_data_name_keyword]}', "
            f"but a second declaration with a value of '{parser.curr_data_value}' "
            f"was encountered at position {parser.curr_match.start()} of the file."
        )

    @staticmethod
    def _table_incomplete(parser: CIFParser):
        return

    @staticmethod
    def _bad_token(parser: CIFParser):
        return f"Bad token: got {parser.curr_token_value} at position {parser.curr_match.span()}"

    @staticmethod
    def _unexpected_token(parser: CIFParser):
        return (
            f"Token out of order: parser is in state {parser.curr_state} "
            f"and expects a token from {_EXPECTED_TOKENS[parser.curr_state]}, "
            f"but received a {parser.curr_token_type}."
        )
