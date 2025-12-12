"""CIF file parser.

This module defines:

- `CIFParser`: Base class for CIF file parsers.


Notes
-----
State diagram of a CIF file parser:

```{mermaid}

stateDiagram-v2

    1 : FILE
    2 : Just DATA
    3 : Just SAVE
    4 : LOOP
    5 : NAME
    6 : JUST SAVE LOOP
    7 : SAVE NAME
    8 : LOOP NAME
    9 : DATA
    10 : SAVE LOOP NAME
    11 : SAVE
    12 : LOOP VALUE
    13 : SAVE LOOP VALUE

    [*] --> 1

    1 --> 2 : DATA
    2 --> 3 : SAVE
    2 --> 4 : LOOP
    2 --> 5 : NAME
    3 --> 6 : LOOP
    3 --> 7 : NAME
    4 --> 8 : NAME
    5 --> 9 : VALUE
    6 --> 10 : NAME
    7 --> 11 : VALUE
    8 --> 8 : NAME
    8 --> 12 : VALUE
    9 --> 2 : DATA
    9 --> 3 : SAVE
    9 --> 4 : LOOP
    9 --> 5 : NAME
    10 --> 10 : NAME
    10 --> 13 : VALUE
    11 --> 9 : SAVE_END
    11 --> 6 : LOOP
    11 --> 7 : NAME
    12 --> 2 : DATA
    12 --> 3 : SAVE
    12 --> 4 : LOOP
    12 --> 5 : NAME
    12 --> 12 : VALUE
    13 --> 9 : SAVE_END
    13 --> 6 : LOOP
    13 --> 7 : NAME
    13 --> 13 : VALUE

    9 --> [*] : EOF
    11 --> [*] : EOF
    12 --> [*] : EOF
    13 --> [*] : EOF
```
"""

import re
from collections.abc import Iterator
from typing import Any, NamedTuple

from ._exception import CIFParsingError, CIFParsingErrorType
from ._token import Token, TOKENIZER
from ._state import State


class SeenCodeInfo(NamedTuple):
    """Information about a seen block/frame code.

    This is used to track the occurrences of data block and save frames
    to generate appropriate error messages in case of duplicates.

    Attributes
    ----------
    idx
        Index of the token in the file where the block/frame code was first seen.
    start
        Start position index of the block/frame code in the file content.
    end
        End position index of the block/frame code in the file content.
    """
    idx: int
    start: int
    end: int


class CIFParser:
    """CIF file parser.

    Notes
    -----
    - Errors are collected and returned, not raised immediately;
      final validation is delegated to caller.
    """

    def __init__(self):
        self._token_processor = {
            Token.VALUE_FIELD: self._process_value_text_field,
            Token.COMMENT: self._process_comment,
            Token.VALUE_QUOTED: self._process_value_quoted,
            Token.VALUE_DOUBLE_QUOTED: self._process_value_double_quoted,
            Token.NAME: self._process_name,
            Token.LOOP: self._process_loop,
            Token.FRAME_CODE: self._process_frame_code,
            Token.FRAME_END: self._process_frame_end,
            Token.BLOCK_CODE: self._process_block_code,
            Token.VALUE: self._process_value,
        }
        """Mapping between token type and its processing method."""

        self._state_mapper = {
            (State.IN_FILE, Token.BLOCK_CODE):           (self._noop, State.JUST_IN_DATA),
            (State.IN_FILE, Token.COMMENT):              (self._noop, State.IN_FILE),
            (State.JUST_IN_DATA, Token.FRAME_CODE):      (self._noop, State.JUST_IN_SAVE),
            (State.JUST_IN_DATA, Token.LOOP):            (self._noop, State.JUST_IN_LOOP),
            (State.JUST_IN_DATA, Token.NAME):            (self._noop, State.IN_NAME),
            (State.JUST_IN_DATA, Token.COMMENT):         (self._noop, State.JUST_IN_DATA),
            (State.JUST_IN_SAVE, Token.LOOP):            (self._noop, State.JUST_IN_SAVE_LOOP),
            (State.JUST_IN_SAVE, Token.NAME):            (self._noop, State.IN_SAVE_NAME),
            (State.JUST_IN_SAVE, Token.COMMENT):         (self._noop, State.JUST_IN_SAVE),
            (State.JUST_IN_LOOP, Token.NAME):            (self._initialize_loop, State.IN_LOOP_NAME),
            (State.JUST_IN_LOOP, Token.COMMENT):         (self._noop, State.JUST_IN_LOOP),
            (State.IN_NAME, Token.VALUE):                (self._add_data_item, State.IN_DATA),
            (State.IN_NAME, Token.COMMENT):              (self._noop, State.IN_NAME),
            (State.JUST_IN_SAVE_LOOP, Token.NAME):       (self._initialize_loop, State.IN_SAVE_LOOP_NAME),
            (State.JUST_IN_SAVE_LOOP, Token.COMMENT):    (self._noop, State.JUST_IN_SAVE_LOOP),
            (State.IN_SAVE_NAME, Token.VALUE):           (self._add_data_item, State.IN_SAVE),
            (State.IN_SAVE_NAME, Token.COMMENT):         (self._noop, State.IN_SAVE_NAME),
            (State.IN_LOOP_NAME, Token.NAME):            (self._add_loop_keyword, State.IN_LOOP_NAME),
            (State.IN_LOOP_NAME, Token.VALUE):           (self._register_and_fill_loop, State.IN_LOOP_VALUE),
            (State.IN_LOOP_NAME, Token.COMMENT):         (self._noop, State.IN_LOOP_NAME),
            (State.IN_DATA, Token.BLOCK_CODE):           (self._noop, State.JUST_IN_DATA),
            (State.IN_DATA, Token.FRAME_CODE):           (self._noop, State.JUST_IN_SAVE),
            (State.IN_DATA, Token.LOOP):                 (self._noop, State.JUST_IN_LOOP),
            (State.IN_DATA, Token.NAME):                 (self._noop, State.IN_NAME),
            (State.IN_DATA, Token.COMMENT):              (self._noop, State.IN_DATA),
            (State.IN_SAVE_LOOP_NAME, Token.NAME):       (self._add_loop_keyword, State.IN_SAVE_LOOP_NAME),
            (State.IN_SAVE_LOOP_NAME, Token.VALUE):      (self._register_and_fill_loop, State.IN_SAVE_LOOP_VALUE),
            (State.IN_SAVE_LOOP_NAME, Token.COMMENT):    (self._noop, State.IN_SAVE_LOOP_NAME),
            (State.IN_SAVE, Token.FRAME_END):            (self._noop, State.IN_DATA),
            (State.IN_SAVE, Token.LOOP):                 (self._noop, State.JUST_IN_SAVE_LOOP),
            (State.IN_SAVE, Token.NAME):                 (self._noop, State.IN_SAVE_NAME),
            (State.IN_SAVE, Token.COMMENT):              (self._noop, State.IN_SAVE),
            (State.IN_LOOP_VALUE, Token.BLOCK_CODE):     (self._finalize_loop, State.JUST_IN_DATA),
            (State.IN_LOOP_VALUE, Token.FRAME_CODE):     (self._finalize_loop, State.JUST_IN_SAVE),
            (State.IN_LOOP_VALUE, Token.LOOP):           (self._finalize_loop, State.JUST_IN_LOOP),
            (State.IN_LOOP_VALUE, Token.NAME):           (self._finalize_loop, State.IN_NAME),
            (State.IN_LOOP_VALUE, Token.VALUE):          (self._fill_loop_value, State.IN_LOOP_VALUE),
            (State.IN_LOOP_VALUE, Token.COMMENT):        (self._noop, State.IN_LOOP_VALUE),
            (State.IN_SAVE_LOOP_VALUE, Token.FRAME_END): (self._finalize_loop, State.IN_DATA),
            (State.IN_SAVE_LOOP_VALUE, Token.LOOP):      (self._finalize_loop, State.JUST_IN_SAVE_LOOP),
            (State.IN_SAVE_LOOP_VALUE, Token.NAME):      (self._finalize_loop, State.IN_SAVE_NAME),
            (State.IN_SAVE_LOOP_VALUE, Token.VALUE):     (self._fill_loop_value, State.IN_SAVE_LOOP_VALUE),
            (State.IN_SAVE_LOOP_VALUE, Token.COMMENT):   (self._noop, State.IN_SAVE_LOOP_VALUE),
        }
        """Mapping between (current state, received token) and (action, resulting state).

        This is a finite state machine that encodes exactly the state diagram shown in the module docstring.
        """

        # Parser state variables
        self.file_content: str = ""
        self.tokenizer: Iterator[re.Match] = None

        self.curr_state: State = State.IN_FILE
        self.curr_token_idx: int = 0
        self.curr_match: re.Match = None
        self.curr_token_type: Token = Token.BAD_TOKEN
        self.curr_token_value: str = ""

        # Current address in the CIF structure and values being processed
        self.curr_block_code: str | None = None
        self.curr_frame_code: str | None = None
        self.curr_frame_code_category: str | None = None
        self.curr_frame_code_keyword: str | None = None
        self.curr_data_name_category: str | None = None
        self.curr_data_name_keyword: str | None = None
        self.curr_data_value: str | None = None

        self.seen_block_codes_in_file: dict[str, SeenCodeInfo] = {}
        self.seen_frame_codes_in_block: dict[str, SeenCodeInfo] = {}

        self.errors: list[CIFParsingError] = []
        return

    # Public Methods
    # ==============

    def parse(self, content: str) -> tuple[Any, list[CIFParsingError]]:
        # Reset parser state variables
        self.file_content = content
        self.tokenizer = TOKENIZER.finditer(self.file_content)

        self.curr_state = State.IN_FILE
        self.curr_token_idx = 0
        self.curr_match = None
        self.curr_token_type = Token.BAD_TOKEN
        self.curr_token_value = None

        self.curr_block_code = None
        self.curr_frame_code = None
        self.curr_frame_code_category = None
        self.curr_frame_code_keyword = None
        self.curr_data_name_category = None
        self.curr_data_name_keyword = None
        self.curr_data_value = None

        self.seen_block_codes_in_file = {}
        self.seen_frame_codes_in_block = {}

        self.errors = []

        # Loop over tokens
        for self.curr_token_idx, self.curr_match in enumerate(self.tokenizer):
            self.curr_token_type = Token(self.curr_match.lastindex)
            self.curr_token_value = self.curr_match.group(self.curr_match.lastindex)

            # Process/normalize token
            processor_func = self._token_processor.get(self.curr_token_type, self._noop)
            processor_func()

            # Store values and update state
            update_func, new_state = self._state_mapper.get(
                (self.curr_state, self.curr_token_type), (self._wrong_token, self.curr_state)
            )
            update_func()
            self.curr_state = new_state

        # Finalize parsing, performing any necessary checks.
        if self.curr_state in (State.IN_LOOP_VALUE, State.IN_SAVE_LOOP_VALUE):
            # End of file reached while in a loop; finalize loop
            self._finalize_loop()
        elif self.curr_state not in (State.IN_DATA, State.IN_SAVE):
            # End of file reached in an invalid state
            self._register_error(CIFParsingErrorType.INCOMPLETE_FILE)
        return self._return_data(), self.errors

    # Private Methods
    # ===============

    # Token Processors
    # ----------------

    def _process_block_code(self) -> None:
        """Process block code token."""
        block_code = self.curr_token_value

        # Set current values
        self.curr_block_code = block_code
        self.curr_frame_code = None
        self.curr_frame_code_category = None
        self.curr_frame_code_keyword = None
        self.curr_data_name_category = None
        self.curr_data_name_keyword = None
        self.curr_data_value = None

        # Update seen codes trackers
        self.seen_frame_codes_in_block = {}
        if block_code in self.seen_block_codes_in_file:
            self._register_error(CIFParsingErrorType.DUPLICATE_BLOCK_CODE)
        self.seen_block_codes_in_file[block_code] = SeenCodeInfo(
            idx=self.curr_token_idx,
            start=self.curr_match.start(),
            end=self.curr_match.end(),
        )
        return

    def _process_frame_code(self) -> None:
        """Process frame code token."""
        frame_code = self.curr_token_value.removeprefix("_")
        frame_code_components = frame_code.split(".", 1)
        frame_code_category = frame_code_components[0]
        frame_code_keyword = frame_code_components[1] if len(frame_code_components) > 1 else None

        # Set current values
        self.curr_frame_code = frame_code
        self.curr_frame_code_category = frame_code_category
        self.curr_frame_code_keyword = frame_code_keyword
        self.curr_data_name_category = None
        self.curr_data_name_keyword = None
        self.curr_data_value = None

        # Update seen codes trackers
        if frame_code in self.seen_frame_codes_in_block:
            self._register_error(CIFParsingErrorType.DUPLICATE)
        self.seen_frame_codes_in_block[frame_code] = SeenCodeInfo(
            idx=self.curr_token_idx,
            start=self.curr_match.start(),
            end=self.curr_match.end(),
        )
        return

    def _process_frame_end(self) -> None:
        """Process frame end token."""
        self.curr_frame_code = None
        self.curr_frame_code_category = None
        self.curr_frame_code_keyword = None
        return

    def _process_loop(self) -> None:
        """Process loop directive token."""
        if self.curr_token_value != "":
            self._register_error(CIFParsingErrorType.LOOP_HAS_NAME)
        return

    def _process_name(self) -> None:
        """Process data name token."""
        name_components = self.curr_token_value.split(".", 1)
        self.curr_data_name_category = name_components[0]
        try:
            self.curr_data_name_keyword = name_components[1]
        except IndexError:
            self.curr_data_name_keyword = None
        return

    def _process_value(self) -> None:
        """Process data value token."""
        self.curr_data_value = self.curr_token_value
        return

    def _process_value_quoted(self) -> None:
        """Process quoted data value token."""
        self.curr_data_value = self.curr_token_value
        self.curr_token_type = Token.VALUE
        return

    def _process_value_double_quoted(self) -> None:
        """Process double-quoted data value token."""
        self.curr_data_value = self.curr_token_value
        self.curr_token_type = Token.VALUE
        return

    def _process_value_text_field(self) -> None:
        """Process text field data value token.

        Notes
        -----
        According to the [spec nr. 17](https://www.iucr.org/resources/cif/spec/version1.1/cifsyntax):
        "Within a multi-line text field,
        leading white space within text lines must be retained as part of the data value;
        trailing white space on a line may however be elided."
        """
        lines = self.curr_token_value.splitlines()
        lines_processed = [line.rstrip() for line in lines]
        self.curr_data_value = "\n".join(lines_processed)
        self.curr_token_type = Token.VALUE
        return

    # State Error Handler
    # -------------------

    def _register_error(self, error_type: CIFParsingErrorType) -> None:
        """
        Given an error type, raise it as a `CIFParsingError` or post a warning message,
        depending on the level of `strictness` and the error level.

        Parameters
        ----------
        error_type : CIFParsingErrorType
            Error type.
        raise_level : {1, 2, 3}
            Minimum strictness level where the error should be raised as an exception.

        Raises
        ------
        CIFParsingError
        """
        self.errors.append(CIFParsingError(parser=self, error_type=error_type))
        return

    # State Update Actions
    # --------------------

    def _wrong_token(self) -> None:
        """Handle unexpected or bad token."""
        if self.curr_token_type == Token.BAD_TOKEN:
            self._register_error(CIFParsingErrorType.BAD_TOKEN)
        elif self.curr_token_type in [Token.STOP, Token.GLOBAL, Token.FRAME_REF, Token.BRACKETS]:
            self._register_error(CIFParsingErrorType.RESERVED_TOKEN)
        else:
            self._register_error(CIFParsingErrorType.UNEXPECTED_TOKEN)
        return

    def _noop(self):
        """No operation."""
        return

    # Abstract methods to be implemented by subclasses

    def _add_data_item(self): ...

    def _initialize_loop(self): ...

    def _add_loop_keyword(self): ...

    def _register_and_fill_loop(self): ...

    def _fill_loop_value(self): ...

    def _finalize_loop(self): ...

    def _return_data(self): ...
