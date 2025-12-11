"""CIF file token types and tokenizer.

This module defines:

- `Token`: An enumeration of different types of tokens
  that can be found in a CIF file.
- `TOKENIZER`: A regular expression (regex)
  used to tokenize a CIF file.
"""

from enum import Enum
import re


__all__ = [
    "Token",
    "TOKENIZER",
]


class Token(Enum):
    """Types of Tokens in a CIF file.

    The values correspond to the index of capturing groups in `TOKENIZER` below.
    """

    VALUE_FIELD = 1  # Will be changed to `VALUE` after processing by parser
    COMMENT = 2
    VALUE_QUOTED = 3  # Will be changed to `VALUE` after processing by parser
    VALUE_DOUBLE_QUOTED = 4  # Will be changed to `VALUE` after processing by parser
    NAME = 5
    LOOP = 6
    DATA = 7
    SAVE = 8
    STOP = 9
    GLOBAL = 10
    FRAME_REF = 11
    BRACKETS = 12
    VALUE = 13
    BAD_TOKEN = 14
    SAVE_END = 15  # Will be added by the parser after processing `SAVE`


TOKENIZER = re.compile(
    r"""(?xmi)  # `x` (cf. re.X) allows for writing the expression in multiple lines, with comments added;
                # `m` (cf. re.M, re.MULTILINE) causes the pattern characters '^' and '$' to also match
                #  the beggining and end of each line, respectively
                #  (in addition to matching the beggining and end of the whole string, respectively).
                # `i` (cf. re.I, re.IGNORECASE) performs case-insensitive matching according to the CIF specification.
    # The following creates different capturing groups (enumerated starting from 1),
    #  each matching one token type. Notice the order of groups matters,
    #  since the matching terminates with the first group match.
    ^;([\S\s]*?)(?:\r\n|\s)^;(?:(?=\s)|$)  # 1. Text field, i.e. a non-simple data value
                                           #    bounded between two '\n;' characters.
    |(?:^|(?<=\s))\#(.*?)\r?$              # 2. Comment
    |(?:^|(?<=\s))(?:
      '(.*?)'                              # 3. Quoted data value
      |"(.*?)"                             # 4. Duble-quoted data value
      |_(\S*)                              # 5. Data name
      |loop_(\S*)                          # 6. Loop header
      |data_(\S*)                          # 7. Block code
      |save_(\S*)                          # 8. Frame code (or terminator)
      |stop_(\S*)                          # 9. STAR-reserved loop terminator
      |global_(\S*)                        # 10. STAR-reserved global block header
      |\$(\S+)                             # 11. STAR-reserved frame reference
      |\[(.+?)]                            # 12. STAR-reserved multi-line value delimeter
      |((?:[^'";_$\[\s]|(?<!^);)\S*)       # 13. Data value
      |(\S+)                               # 14. Bad token (anything else)
    )
    (?:(?=\s)|$)"""
)
"""CIF file tokenizer regular expression (regex).

This is compiled regex to capture tokens in an mmCIF file.
It can be used on a single multi-line string
representing the whole content of an mmCIF file.
Used in iterative mode, it will then tokenize the whole file
(tokens are separated by any whitespace character
that is not encapsulated in a non-simple data value delimiter,
as described in the CIF documentation),
and identify the type of each captured token.
"""
