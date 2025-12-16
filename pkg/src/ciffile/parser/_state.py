"""CIF file parser states.

This module defines:

- `State`: An enumeration of the possible states
  of the CIF parser.
"""


from enum import Enum, auto


__all__ = [
    "State",
]


class State(Enum):
    """Possible states of the parser in a CIF file.

    Attributes
    ----------
    IN_FILE
        The initial state; the parser is in the file
        but has not yet encountered the first data block header.
        In this state, the only expected token (beside comments, which are always expected)
        is a data block header, i.e. `Token.DATA`.
    JUST_IN_DATA
        The parser has just encountered a data block header,
        and is now expecting either a save frame header (i.e. `Token.SAVE`),
        a loop directive (i.e. `Token.LOOP`), or a data name (i.e. `Token.NAME`).
    JUST_IN_SAVE
        The parser has just encountered a save frame header,
        and is now expecting either a loop directive, or a data name.
    JUST_IN_LOOP
        The parser has just encountered a loop directive,
        and is now expecting a data name.
    IN_NAME
        The parser was in a data block and has just encountered a data name.
        It is now expecting a data value, i.e. either
        `Token.VALUE`, `Token.VALUE_QUOTED` or `Token.VALUE_FIELD`.
    JUST_IN_SAVE_LOOP
        The parser was in a save frame and has just encountered a loop directive.
        It is now expecting a data name.
    IN_SAVE_NAME
        The parser was in a save frame and has just encountered a data name.
        It is now expecting a data value.
    IN_LOOP_NAME
        The parser was in a loop and has just encountered the first or n-th data name.
        It is now expecting either another data name, or the first data value.
    IN_DATA
        The parser was either in a save frame and has just encountered a save frame termination directive
        (i.e. `Token.SAVE_END`), or it was in a data block and has just finished parsing a data item
        (i.e. it encountered a data name, followed by a data value). It is now in a data block
        and expecting either another data block header, a save frame header, a loop directive,
        or a data name.
    IN_SAVE_LOOP_NAME
        The parser was in a loop inside a save frame and has just encountered the first or n-th data name.
        It is now expecting either another data name, or the first data value.
    IN_SAVE
        The parser has just encountered a data value (directly after a data name) within a save frame.
        It is now inside the save frame expecting either a save frame termination directive, a loop directive,
        or a data name.
    IN_LOOP_VALUE
        The parser has encountered the first or n-th data value,
        while being previously in state `IN_LOOP_NAME`. It is now expecting either another data value,
        a new data block header, a save frame header, another loop directive, or a data name.
    IN_SAVE_LOOP_VALUE
        The parser has encountered the first or n-th data value,
        while being previously in state `IN_SAVE_LOOP_NAME`. It is now expecting either another data value,
        a save frame termination directive, another loop directive, or a data name.
    """

    IN_FILE = auto()
    JUST_IN_DATA = auto()
    JUST_IN_SAVE = auto()
    JUST_IN_LOOP = auto()
    IN_NAME = auto()
    JUST_IN_SAVE_LOOP = auto()
    IN_SAVE_NAME = auto()
    IN_LOOP_NAME = auto()
    IN_DATA = auto()
    IN_SAVE_LOOP_NAME = auto()
    IN_SAVE = auto()
    IN_LOOP_VALUE = auto()
    IN_SAVE_LOOP_VALUE = auto()
