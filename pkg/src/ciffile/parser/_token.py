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

    VALUE_FIELD = 1
    COMMENT = 2
    VALUE_QUOTED = 3
    VALUE_DOUBLE_QUOTED = 4
    NAME = 5
    LOOP = 6
    FRAME_CODE = 7
    FRAME_END = 8
    BLOCK_CODE = 9
    STOP = 10
    GLOBAL = 11
    FRAME_REF = 12
    BRACKETS = 13
    VALUE = 14
    BAD_TOKEN = 15


TOKENIZER = re.compile(
    r"""(?xmi)  # `x` (cf. re.X) allows for writing the expression in multiple lines, with comments added;
                # `m` (cf. re.M, re.MULTILINE) causes the pattern characters '^' and '$' to also match
                #  the beggining and end of each line, respectively
                #  (in addition to matching the beggining and end of the whole string, respectively).
                # `i` (cf. re.I, re.IGNORECASE) performs case-insensitive matching according to the CIF specification.
    # The following creates different capturing groups (enumerated starting from 1),
    #  each matching one token type. Notice the order of groups matters,
    #  since the matching terminates with the first group match.
    ^;([\S\s]*?)(?:\r\n|\s)^;(?:(?=\s)|$)  # 1. Text field, i.e. a non-simple data value bounded between two '<eol>;' characters.
    |(?:^|(?<=\s))\#(.*?)\r?$              # 2. Comment
    |(?:^|(?<=\s))(?:
      '(.*?)'                              # 3. Quoted data value
      |"(.*?)"                             # 4. Double-quoted data value
      |_(\S*)                              # 5. Data name
      |loop_(\S*)                          # 6. Loop header
      |save_(\S+)                          # 7. Frame code
      |save_()                             # 8. Frame end
      |data_(\S*)                          # 9. Block code
      |stop_(\S*)                          # 10. STAR-reserved loop terminator
      |global_(\S*)                        # 11. STAR-reserved global block header
      |\$(\S+)                             # 12. STAR-reserved frame reference
      |\[(.+?)]                            # 13. STAR-reserved multi-line value delimeter
      |((?:[^'";_$\[\s]|(?<!^);)\S*)       # 14. Data value
      |(\S+)                               # 15. Bad token (anything else)
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
