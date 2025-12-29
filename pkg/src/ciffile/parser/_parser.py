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
from collections.abc import Iterator, Callable
from typing import NamedTuple, Literal

from fileex.file import content as filelike_to_str
from tqdm.auto import tqdm

from ciffile.typing import FileLike
from ._exception import CIFFileParseError, CIFFileParseErrorType
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

    def __init__(
        self,
        file: FileLike,
        *,
        variant: Literal["cif1", "mmcif"] = "mmcif",
        encoding: str = "utf-8",
        case_normalization: Literal["lower", "upper"] | None = "lower",
        raise_level: Literal[0, 1, 2] = 2,
    ) -> None:
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

        self._file: FileLike = file
        self._variant: Literal["cif1", "mmcif"] = variant
        self._case_normalization: Literal["lower", "upper"] | None = case_normalization
        self._raise_level: Literal[0, 1, 2] = raise_level
        self._tokenizer: Iterator[re.Match] = TOKENIZER.finditer(
            filelike_to_str(file, output="str", encoding=encoding)
        )
        self._case_normalizer: Callable[[str], str] = {
            "lower": lambda s: s.lower() if s else s,
            "upper": lambda s: s.upper() if s else s,
            None: lambda s: s,
        }[self._case_normalization]

        # Parser state variables
        self._curr_state: State = State.IN_FILE
        self._curr_token_idx: int = 0
        self._curr_match: re.Match = None
        self._curr_token_type: Token = Token.BAD_TOKEN
        self._curr_token_value: str = ""

        # Current address in the CIF structure and values being processed
        self._curr_block_code: str | None = None
        self._curr_frame_code: str | None = None
        self._curr_data_name: str | None = None
        self._curr_data_category: str | None = None
        self._curr_data_keyword: str | None = None
        self._curr_data_value: str | None = None

        self._seen_block_codes_in_file: dict[str, SeenCodeInfo] = {}
        self._seen_frame_codes_in_block: dict[str, SeenCodeInfo] = {}
        self._seen_data_names_in_block: dict[str, SeenCodeInfo] = {}
        self._seen_data_categories_in_block: dict[str, SeenCodeInfo] = {}
        self._seen_data_names_in_frame: dict[str, SeenCodeInfo] = {}
        self._seen_data_categories_in_frame: dict[str, SeenCodeInfo] = {}
        self._seen_table_categories_in_block: dict[str, SeenCodeInfo] = {}
        self._seen_table_categories_in_frame: dict[str, SeenCodeInfo] = {}

        self._output_block_codes: list[str] = []
        self._output_frame_codes: list[str | None] = []
        self._output_data_categories: list[str | None] = []
        self._output_data_keywords: list[str] = []
        self._output_data_values: list[list[str]] = []

        self._loop_value_lists: itertools.cycle = None
        self._loop_value_lists_idx: itertools.cycle = None

        self._curr_loop_id: int = 0
        self._curr_loop_columns: list[list[str]] = []

        # Public attributes
        self.errors: list[CIFFileParseError] = []
        self.output: CIFFlatDict = self._parse()

        return

    # Private Methods
    # ===============

    def _parse(self) -> CIFFlatDict:
        NOOP = lambda: None

        # Loop over tokens
        for self._curr_token_idx, self._curr_match in tqdm(
                enumerate(self._tokenizer),
                desc=f"Parsing CIF",
                unit="tokens"
            ):
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
            self._register_error(CIFFileParseErrorType.FILE_INCOMPLETE)

        return CIFFlatDict(
            block=self._output_block_codes,
            frame=self._output_frame_codes,
            category=self._output_data_categories,
            keyword=self._output_data_keywords,
            values=self._output_data_values,
        )

    # State Update Actions
    # --------------------

    def _new_data_block(self) -> None:
        """Initialize a new data block."""
        block_code = self._curr_token_value

        # Set current values
        self._reset_currents("block")
        self._curr_block_code = block_code

        if block_code == "":
            self._register_error(CIFFileParseErrorType.BLOCK_CODE_EMPTY)
        if block_code in self._seen_block_codes_in_file:
            self._register_error(CIFFileParseErrorType.BLOCK_CODE_DUPLICATE)

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
        self._reset_currents("frame")
        self._curr_frame_code = frame_code

        if frame_code == "":
            self._register_error(CIFFileParseErrorType.FRAME_CODE_EMPTY)
        if frame_code in self._seen_frame_codes_in_block:
            self._register_error(CIFFileParseErrorType.FRAME_CODE_DUPLICATE)

        self._seen_frame_codes_in_block[frame_code] = SeenCodeInfo(
            idx=self._curr_token_idx,
            start=self._curr_match.start(),
            end=self._curr_match.end(),
        )
        return

    def _new_loop(self) -> None:
        """Initialize a new loop."""
        self._reset_currents("loop")
        loop_code = self._curr_token_value
        self._curr_loop_id += 1
        self._curr_loop_columns = []

        if loop_code != "":
            self._register_error(CIFFileParseErrorType.LOOP_NAMED)
        return

    def _new_name_in_data_block(self) -> None:
        """Initialize a new data name in the current data block."""
        return self._new_name(
            seen_names=self._seen_data_names_in_block,
            seen_categories=self._seen_data_categories_in_block,
            seen_tables=self._seen_table_categories_in_block,
        )

    def _new_name_in_save_frame(self) -> None:
        """Initialize a new data name in the current save frame."""
        return self._new_name(
            seen_names=self._seen_data_names_in_frame,
            seen_categories=self._seen_data_categories_in_frame,
            seen_tables=self._seen_table_categories_in_frame
        )

    def _new_name_in_loop(self) -> None:
        """Initialize a new data name in the current loop."""
        seen_names, seen_categories, seen_tables = self._get_seen_dicts()
        self._new_name(
            seen_names=seen_names,
            seen_categories=seen_categories,
            seen_tables=seen_tables,
            loop_id=self._curr_loop_id
        )
        new_column = []
        self._curr_loop_columns.append(new_column)
        self._add_data(data_value=new_column)
        return

    def _new_value(self) -> None:
        self._curr_data_value = self._curr_token_value
        self._add_data(data_value=[self._curr_data_value])
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
        if self._variant == "mmcif":
            _, seen_categories, seen_tables = self._get_seen_dicts()
            cat = self._curr_data_category
            for seen in (seen_categories, seen_tables):
                seen[cat] = SeenCodeInfo(
                    idx=self._curr_token_idx,
                    start=self._curr_match.start(),
                    end=self._curr_match.end(),
                )
        return

    def _end_loop(self):
        if next(self._loop_value_lists_idx) != 0:
            self._register_error(CIFFileParseErrorType.TABLE_INCOMPLETE)
        self._reset_currents("loop")
        return

    def _end_save_frame(self) -> None:
        """Process frame end token."""
        self._reset_currents("frame")
        return

    def _wrong_token(self) -> None:
        """Handle unexpected or bad token."""
        if self._curr_token_type == Token.BAD_TOKEN:
            self._register_error(CIFFileParseErrorType.TOKEN_BAD)
        elif self._curr_token_type in [Token.STOP, Token.GLOBAL, Token.FRAME_REF, Token.BRACKETS]:
            self._register_error(CIFFileParseErrorType.TOKEN_RESERVED)
        else:
            self._register_error(CIFFileParseErrorType.TOKEN_UNEXPECTED)
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

    def _register_error(
        self,
        error_type: CIFFileParseErrorType,
        *,
        state: State | None = None,
        token_idx: int | None = None,
        match: re.Match | None = None,
        token_type: Token | None = None,
        token_value: str | None = None,
        block_code: str | None = None,
        frame_code: str | None = None,
        data_category: str | None = None,
        data_keyword: str | None = None,
        data_name: str | None = None,
        data_value: str | None = None,
        seen_block_codes: dict[str, SeenCodeInfo] | None = None,
        seen_frame_codes: dict[str, SeenCodeInfo] | None = None,
        seen_data_names_in_block: dict[str, SeenCodeInfo] | None = None,
        seen_data_names_in_frame: dict[str, SeenCodeInfo] | None = None,
        seen_data_categories_in_block: dict[str, SeenCodeInfo] | None = None,
        seen_data_categories_in_frame: dict[str, SeenCodeInfo] | None = None,
        seen_table_categories_in_block: dict[str, SeenCodeInfo] | None = None,
        seen_table_categories_in_frame: dict[str, SeenCodeInfo] | None = None,
        expected_tokens: list[Token] | None = None,
    ) -> None:
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
        error_kwargs = {
            "state": self._curr_state,
            "token_idx": self._curr_token_idx,
            "match": self._curr_match,
            "token_type": self._curr_token_type,
            "token_value": self._curr_token_value,
            "block_code": self._curr_block_code,
            "frame_code": self._curr_frame_code,
            "data_category": self._curr_data_category,
            "data_keyword": self._curr_data_keyword,
            "data_name": self._curr_data_name,
            "data_value": self._curr_data_value,
            "seen_block_codes": self._seen_block_codes_in_file.copy(),
            "seen_frame_codes": self._seen_frame_codes_in_block.copy(),
            "seen_data_names_in_block": self._seen_data_names_in_block.copy(),
            "seen_data_names_in_frame": self._seen_data_names_in_frame.copy(),
            "seen_data_categories_in_block": self._seen_data_categories_in_block.copy(),
            "seen_data_categories_in_frame": self._seen_data_categories_in_frame.copy(),
            "seen_table_categories_in_block": self._seen_table_categories_in_block.copy(),
            "seen_table_categories_in_frame": self._seen_table_categories_in_frame.copy(),
            "expected_tokens": [
                token for state, token in self._state_mapper.keys()
                if state == self._curr_state
            ],
        } | {
            k: v for k, v in locals().items()
            if v is not None and k != "self"
        }
        error = CIFFileParseError(**error_kwargs)
        self.errors.append(error)
        return

    # Private Helper Methods
    # ======================

    def _new_name(
        self,
        seen_names: dict[str, SeenCodeInfo],
        seen_categories: dict[str, SeenCodeInfo],
        seen_tables: dict[str, SeenCodeInfo],
        loop_id: int | None = None,
    ) -> None:
        """Initialize a new data name."""
        data_name = self._curr_token_value

        # Set current values
        self._curr_data_name = data_name
        self._curr_data_value = None

        if self._variant == "cif1":
            # In CIF 1.1, there is no explicit category.keyword syntax.
            # Preserve loop/table grouping by assigning a synthetic category based on loop_id,
            # and for single (non-loop) items, use the data name
            # as the category to allow direct addressing (e.g., block["item"]).
            self._curr_data_category = data_name if loop_id is None else str(loop_id)
            # Use data_name as keyword for both single and loop items
            self._curr_data_keyword = data_name
        elif self._variant == "mmcif":
            last_data_category = self._curr_data_category
            parts = data_name.split(".")
            period_count = len(parts) - 1
            if period_count == 0:
                self._curr_data_category = None
                self._curr_data_keyword = data_name
            else:
                self._curr_data_category = parts[0]
                self._curr_data_keyword = ".".join(parts[1:])

            if period_count != 1:
                self._register_error(CIFFileParseErrorType.DATA_NAME_NOT_MMCIF)
            if self._curr_data_category in seen_tables:
                self._register_error(CIFFileParseErrorType.TABLE_CAT_REPEATED)

            if loop_id is None:
                seen_categories[self._curr_data_category] = SeenCodeInfo(
                    idx=self._curr_token_idx,
                    start=self._curr_match.start(),
                    end=self._curr_match.end(),
                )
            else:
                if self._curr_data_category in seen_categories:
                    self._register_error(CIFFileParseErrorType.TABLE_CAT_DUPLICATE)
                if last_data_category is not None and self._curr_data_category != last_data_category:
                    self._register_error(CIFFileParseErrorType.TABLE_MULTICAT)

        if data_name == "":
            self._register_error(CIFFileParseErrorType.DATA_NAME_EMPTY)
        if data_name in seen_names:
            self._register_error(CIFFileParseErrorType.DATA_NAME_DUPLICATE)
        seen_names[data_name] = SeenCodeInfo(
            idx=self._curr_token_idx,
            start=self._curr_match.start(),
            end=self._curr_match.end(),
        )
        return

    def _add_data(self, data_value: str | list):
        for output, output_list in (
            (self._curr_block_code, self._output_block_codes),
            (self._curr_frame_code, self._output_frame_codes),
            (self._curr_data_category, self._output_data_categories),
            (self._curr_data_keyword, self._output_data_keywords),
        ):
            output_list.append(self._case_normalizer(output))
        self._output_data_values.append(data_value)
        return

    def _reset_currents(self, level: Literal["block", "frame", "loop"]) -> None:
        """Reset parser state to a given level.

        Parameters
        ----------
        level
            Level to reset to; one of:
            - "block": Reset to new data block level.
            - "frame": Reset to new save frame level.
            - "loop": Reset to new loop level.
        """
        l = {"block": 0, "frame": 1, "loop": 2}[level]
        self._curr_data_name = None
        self._curr_data_category = None
        self._curr_data_keyword = None
        self._curr_data_value = None
        if l < 2:
            self._curr_frame_code = None
            self._seen_table_categories_in_frame = {}
            self._seen_data_names_in_frame = {}
            self._seen_data_categories_in_frame = {}
        if l < 1:
            self._curr_block_code = None
            self._seen_frame_codes_in_block = {}
            self._seen_table_categories_in_block = {}
            self._seen_data_names_in_block = {}
            self._seen_data_categories_in_block = {}
        return

    def _get_seen_dicts(self) -> tuple[dict[str, SeenCodeInfo], dict[str, SeenCodeInfo], dict[str, SeenCodeInfo]]:
        """Get the appropriate seen code dictionaries based on the current context.

        Returns
        -------
        seen_data_names
            Seen data names dictionary.
        seen_data_categories
            Seen data categories dictionary.
        seen_table_categories
            Seen table categories dictionary.
        """
        return (
            self._seen_data_names_in_block,
            self._seen_data_categories_in_block,
            self._seen_table_categories_in_block,
        ) if self._curr_frame_code is None else (
            self._seen_data_names_in_frame,
            self._seen_data_categories_in_frame,
            self._seen_table_categories_in_frame,
        )