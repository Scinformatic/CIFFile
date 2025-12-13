"""CIF file parser.

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

import itertools
import re
from collections.abc import Iterator
from typing import NamedTuple

from ._exception import CIFParsingError, CIFParsingErrorType
from ._output import CIFFlatDict
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

    def __init__(self, file: str):
        NOOP = lambda: None

        self._token_preprocessors = {
            Token.VALUE_FIELD: self._process_value_text_field,
            Token.VALUE_QUOTED: self._process_value_quoted,
            Token.VALUE_DOUBLE_QUOTED: self._process_value_double_quoted,
        }

        self._state_mapper = {
            (State.IN_FILE, Token.BLOCK_CODE):           (NOOP, self._new_data_block, State.JUST_IN_DATA),
            (State.IN_FILE, Token.COMMENT):              (NOOP, NOOP, State.IN_FILE),
            (State.JUST_IN_DATA, Token.FRAME_CODE):      (NOOP, self._new_save_frame, State.JUST_IN_SAVE),
            (State.JUST_IN_DATA, Token.LOOP):            (NOOP, self._new_loop, State.JUST_IN_LOOP),
            (State.JUST_IN_DATA, Token.NAME):            (NOOP, self._new_name_in_data_block, State.IN_NAME),
            (State.JUST_IN_DATA, Token.COMMENT):         (NOOP, NOOP, State.JUST_IN_DATA),
            (State.JUST_IN_SAVE, Token.LOOP):            (NOOP, self._new_loop, State.JUST_IN_SAVE_LOOP),
            (State.JUST_IN_SAVE, Token.NAME):            (NOOP, self._new_name_in_save_frame, State.IN_SAVE_NAME),
            (State.JUST_IN_SAVE, Token.COMMENT):         (NOOP, NOOP, State.JUST_IN_SAVE),
            (State.JUST_IN_LOOP, Token.NAME):            (NOOP, self._new_name_in_loop, State.IN_LOOP_NAME),
            (State.JUST_IN_LOOP, Token.COMMENT):         (NOOP, NOOP, State.JUST_IN_LOOP),
            (State.IN_NAME, Token.VALUE):                (NOOP, self._new_value, State.IN_DATA),
            (State.IN_NAME, Token.COMMENT):              (NOOP, NOOP, State.IN_NAME),
            (State.JUST_IN_SAVE_LOOP, Token.NAME):       (NOOP, self._new_name_in_loop, State.IN_SAVE_LOOP_NAME),
            (State.JUST_IN_SAVE_LOOP, Token.COMMENT):    (NOOP, NOOP, State.JUST_IN_SAVE_LOOP),
            (State.IN_SAVE_NAME, Token.VALUE):           (NOOP, self._new_value, State.IN_SAVE),
            (State.IN_SAVE_NAME, Token.COMMENT):         (NOOP, NOOP, State.IN_SAVE_NAME),
            (State.IN_LOOP_NAME, Token.NAME):            (NOOP, self._new_name_in_loop, State.IN_LOOP_NAME),
            (State.IN_LOOP_NAME, Token.VALUE):           (self._end_loop_header, self._new_value_in_loop, State.IN_LOOP_VALUE),
            (State.IN_LOOP_NAME, Token.COMMENT):         (NOOP, NOOP, State.IN_LOOP_NAME),
            (State.IN_DATA, Token.BLOCK_CODE):           (NOOP, self._new_data_block, State.JUST_IN_DATA),
            (State.IN_DATA, Token.FRAME_CODE):           (NOOP, self._new_save_frame, State.JUST_IN_SAVE),
            (State.IN_DATA, Token.LOOP):                 (NOOP, self._new_loop, State.JUST_IN_LOOP),
            (State.IN_DATA, Token.NAME):                 (NOOP, self._new_name_in_data_block, State.IN_NAME),
            (State.IN_DATA, Token.COMMENT):              (NOOP, NOOP, State.IN_DATA),
            (State.IN_SAVE_LOOP_NAME, Token.NAME):       (NOOP, self._new_name_in_loop, State.IN_SAVE_LOOP_NAME),
            (State.IN_SAVE_LOOP_NAME, Token.VALUE):      (self._end_loop_header, self._new_value_in_loop, State.IN_SAVE_LOOP_VALUE),
            (State.IN_SAVE_LOOP_NAME, Token.COMMENT):    (NOOP, NOOP, State.IN_SAVE_LOOP_NAME),
            (State.IN_SAVE, Token.FRAME_END):            (NOOP, self._end_save_frame, State.IN_DATA),
            (State.IN_SAVE, Token.LOOP):                 (NOOP, self._new_loop, State.JUST_IN_SAVE_LOOP),
            (State.IN_SAVE, Token.NAME):                 (NOOP, self._new_name_in_save_frame, State.IN_SAVE_NAME),
            (State.IN_SAVE, Token.COMMENT):              (NOOP, NOOP, State.IN_SAVE),
            (State.IN_LOOP_VALUE, Token.BLOCK_CODE):     (self._end_loop, self._new_data_block, State.JUST_IN_DATA),
            (State.IN_LOOP_VALUE, Token.FRAME_CODE):     (self._end_loop, self._new_save_frame, State.JUST_IN_SAVE),
            (State.IN_LOOP_VALUE, Token.LOOP):           (self._end_loop, self._new_loop, State.JUST_IN_LOOP),
            (State.IN_LOOP_VALUE, Token.NAME):           (self._end_loop, self._new_name_in_data_block, State.IN_NAME),
            (State.IN_LOOP_VALUE, Token.VALUE):          (NOOP, self._new_value_in_loop, State.IN_LOOP_VALUE),
            (State.IN_LOOP_VALUE, Token.COMMENT):        (NOOP, NOOP, State.IN_LOOP_VALUE),
            (State.IN_SAVE_LOOP_VALUE, Token.FRAME_END): (self._end_loop, self._end_save_frame, State.IN_DATA),
            (State.IN_SAVE_LOOP_VALUE, Token.LOOP):      (self._end_loop, self._new_loop, State.JUST_IN_SAVE_LOOP),
            (State.IN_SAVE_LOOP_VALUE, Token.NAME):      (self._end_loop, self._new_name_in_save_frame, State.IN_SAVE_NAME),
            (State.IN_SAVE_LOOP_VALUE, Token.VALUE):     (NOOP, self._new_value_in_loop, State.IN_SAVE_LOOP_VALUE),
            (State.IN_SAVE_LOOP_VALUE, Token.COMMENT):   (NOOP, NOOP, State.IN_SAVE_LOOP_VALUE),
        }
        """Mapping between (current state, received token) and (action, resulting state).

        This is a finite state machine that encodes exactly the state diagram shown in the module docstring.
        """

        # Parser state variables
        self._file: str = file
        self._tokenizer: Iterator[re.Match] = TOKENIZER.finditer(self._file)

        self._curr_state: State = State.IN_FILE
        self._curr_token_idx: int = 0
        self._curr_match: re.Match = None
        self._curr_token_type: Token = Token.BAD_TOKEN
        self._curr_token_value: str = ""

        # Current address in the CIF structure and values being processed
        self._curr_block_code: str | None = None
        self._curr_frame_code: str | None = None
        self._curr_data_name: str | None = None
        self._curr_data_value: str | None = None

        self._seen_block_codes_in_file: dict[str, SeenCodeInfo] = {}
        self._seen_frame_codes_in_block: dict[str, SeenCodeInfo] = {}
        self._seen_data_names_in_block: dict[str, SeenCodeInfo] = {}
        self._seen_data_names_in_frame: dict[str, SeenCodeInfo] = {}

        self._output_block_codes: list[str] = []
        self._output_frame_codes: list[str | None] = []
        self._output_loop_codes: list[int] = []
        self._output_data_names: list[str] = []
        self._output_data_values: list[list[str]] = []

        self._loop_value_lists: itertools.cycle = None
        self._loop_value_lists_idx: itertools.cycle = None

        self._curr_loop_id: int = 0
        self._curr_loop_columns: list[list[str]] = []

        # Public attributes
        self.errors: list[CIFParsingError] = []
        self.output: CIFFlatDict = self._parse()

        return

    # Private Methods
    # ===============

    def _parse(self) -> CIFFlatDict:
        NOOP = lambda: None

        # Loop over tokens
        for self._curr_token_idx, self._curr_match in enumerate(self._tokenizer):
            self._curr_token_type = Token(self._curr_match.lastindex)
            self._curr_token_value = self._curr_match.group(self._curr_match.lastindex)

            # Preprocess token if needed
            preprocessor = self._token_preprocessors.get(self._curr_token_type, NOOP)
            preprocessor()

            # Store values and update state
            curr_state_updater, new_state_updater, new_state = self._state_mapper.get(
                (self._curr_state, self._curr_token_type), (self._wrong_token, NOOP, self._curr_state)
            )
            curr_state_updater()
            new_state_updater()
            self._curr_state = new_state

        # Finalize parsing, performing any necessary checks.
        if self._curr_state in (State.IN_LOOP_VALUE, State.IN_SAVE_LOOP_VALUE):
            # End of file reached while in a loop; finalize loop
            self._end_loop()
        elif self._curr_state not in (State.IN_DATA, State.IN_SAVE):
            # End of file reached in an invalid state
            self._register_error(CIFParsingErrorType.FILE_INCOMPLETE)

        output = CIFFlatDict(
            block_code=self._output_block_codes,
            frame_code=self._output_frame_codes,
            loop_code=self._output_loop_codes,
            data_name=self._output_data_names,
            data_values=self._output_data_values,
        )
        return output

    # State Update Actions
    # --------------------

    def _new_data_block(self) -> None:
        """Initialize a new data block."""
        block_code = self._curr_token_value

        # Set current values
        self._curr_block_code = block_code
        self._curr_frame_code = None
        self._curr_data_name = None
        self._curr_data_value = None

        # Update seen codes trackers
        self._seen_frame_codes_in_block = {}
        self._seen_data_names_in_block = {}
        self._seen_data_names_in_frame = {}

        if block_code == "":
            self._register_error(CIFParsingErrorType.BLOCK_CODE_EMPTY)
        if block_code in self._seen_block_codes_in_file:
            self._register_error(CIFParsingErrorType.BLOCK_CODE_DUPLICATE)

        self._seen_block_codes_in_file[block_code] = SeenCodeInfo(
            idx=self._curr_token_idx,
            start=self._curr_match.start(),
            end=self._curr_match.end(),
        )
        return

    def _new_save_frame(self) -> None:
        """Initialize a new save frame."""
        frame_code = self._curr_token_value.removeprefix("_")

        # Set current values
        self._curr_frame_code = frame_code
        self._curr_data_name = None
        self._curr_data_value = None

        # Update seen codes trackers
        self._seen_data_names_in_frame = {}

        if frame_code == "":
            self._register_error(CIFParsingErrorType.FRAME_CODE_EMPTY)
        if frame_code in self._seen_frame_codes_in_block:
            self._register_error(CIFParsingErrorType.FRAME_CODE_DUPLICATE)

        self._seen_frame_codes_in_block[frame_code] = SeenCodeInfo(
            idx=self._curr_token_idx,
            start=self._curr_match.start(),
            end=self._curr_match.end(),
        )
        return

    def _new_loop(self) -> None:
        """Initialize a new loop."""
        loop_code = self._curr_token_value

        self._curr_data_name = None
        self._curr_data_value = None

        self._curr_loop_id += 1
        self._curr_loop_columns = []

        if loop_code != "":
            self._register_error(CIFParsingErrorType.LOOP_NAMED)
        return

    def _new_name_in_data_block(self) -> None:
        """Initialize a new data name in the current data block."""
        return self._new_name(self._seen_data_names_in_block)

    def _new_name_in_save_frame(self) -> None:
        """Initialize a new data name in the current save frame."""
        return self._new_name(self._seen_data_names_in_frame)

    def _new_name_in_loop(self) -> None:
        """Initialize a new data name in the current loop."""
        self._new_name(self._seen_data_names_in_block if self._curr_frame_code is None else self._seen_data_names_in_frame)
        new_column = []
        self._curr_loop_columns.append(new_column)
        self._add_data(data_value=new_column, loop_id=self._curr_loop_id)
        return

    def _new_value(self) -> None:
        self._curr_data_value = self._curr_token_value
        self._add_data(data_value=[self._curr_data_value], loop_id=0)
        return

    def _new_value_in_loop(self) -> None:
        """Initialize a new data value in the current loop."""
        self._curr_data_value = self._curr_token_value
        next(self._loop_value_lists).append(self._curr_data_value)
        next(self._loop_value_lists_idx)
        return

    def _end_loop_header(self) -> None:
        """Finalize loop header processing."""
        self._loop_value_lists = itertools.cycle(self._curr_loop_columns)
        self._loop_value_lists_idx = itertools.cycle(range(len(self._curr_loop_columns)))
        return

    def _end_loop(self):
        if next(self._loop_value_lists_idx) != 0:
            self._register_error(CIFParsingErrorType.TABLE_INCOMPLETE)
        return

    def _end_save_frame(self) -> None:
        """Process frame end token."""
        self._curr_frame_code = None
        self._curr_data_name = None
        self._curr_data_value = None

        self._seen_data_names_in_frame = {}
        return

    def _wrong_token(self) -> None:
        """Handle unexpected or bad token."""
        if self._curr_token_type == Token.BAD_TOKEN:
            self._register_error(CIFParsingErrorType.TOKEN_BAD)
        elif self._curr_token_type in [Token.STOP, Token.GLOBAL, Token.FRAME_REF, Token.BRACKETS]:
            self._register_error(CIFParsingErrorType.TOKEN_RESERVED)
        else:
            self._register_error(CIFParsingErrorType.TOKEN_UNEXPECTED)
        return

    # Token Processors
    # ----------------

    def _process_value_text_field(self) -> None:
        """Process text field data value token.

        Notes
        -----
        According to the [spec nr. 17](https://www.iucr.org/resources/cif/spec/version1.1/cifsyntax):
        "Within a multi-line text field,
        leading white space within text lines must be retained as part of the data value;
        trailing white space on a line may however be elided."
        """
        lines = self._curr_token_value.splitlines()
        lines_processed = [line.rstrip() for line in lines]
        self._curr_token_value = "\n".join(lines_processed)
        self._curr_token_type = Token.VALUE
        return

    def _process_value_quoted(self) -> None:
        """Process quoted data value token."""
        self._curr_token_type = Token.VALUE
        return

    def _process_value_double_quoted(self) -> None:
        """Process double-quoted data value token."""
        self._curr_token_type = Token.VALUE
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
        error = CIFParsingError(
            error_type=error_type,
            state=self._curr_state,
            token_idx=self._curr_token_idx,
            match=self._curr_match,
            token_type=self._curr_token_type,
            token_value=self._curr_token_value,
            block_code=self._curr_block_code,
            frame_code=self._curr_frame_code,
            data_name=self._curr_data_name,
            data_value=self._curr_data_value,
            seen_block_codes=self._seen_block_codes_in_file.copy(),
            seen_frame_codes=self._seen_frame_codes_in_block.copy(),
            seen_data_names_in_block=self._seen_data_names_in_block.copy(),
            seen_data_names_in_frame=self._seen_data_names_in_frame.copy(),
            expected_tokens=[
                token for state, token in self._state_mapper.keys()
                if state == self._curr_state
            ]
        )
        self.errors.append(error)
        return

    # Private Helper Methods
    # ======================

    def _new_name(self, seen_names: dict[str, SeenCodeInfo]) -> None:
        """Initialize a new data name."""
        data_name = self._curr_token_value

        # Set current values
        self._curr_data_name = data_name
        self._curr_data_value = None

        if data_name == "":
            self._register_error(CIFParsingErrorType.DATA_NAME_EMPTY)

        if data_name in seen_names:
            self._register_error(CIFParsingErrorType.DATA_NAME_DUPLICATE)
        seen_names[data_name] = SeenCodeInfo(
            idx=self._curr_token_idx,
            start=self._curr_match.start(),
            end=self._curr_match.end(),
        )
        return

    def _add_data(self, data_value: str | list, loop_id: int):
        self._output_block_codes.append(self._curr_block_code)
        self._output_frame_codes.append(self._curr_frame_code)
        self._output_loop_codes.append(loop_id)
        self._output_data_names.append(self._curr_data_name)
        self._output_data_values.append(data_value)
        return
