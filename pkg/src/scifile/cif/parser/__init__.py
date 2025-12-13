"""CIF file parser."""

from scifile.typing import FileLike
from scifile._util import filelike_to_str
from ._parser import CIFParser
from ._output import CIFFlatDict
from ._exception import CIFParsingError

__all__ = [
    "CIFFlatDict",
    "CIFParsingError",
    "parse",
]


def parse(file: FileLike, encoding: str = "utf-8") -> tuple[CIFFlatDict, list[CIFParsingError]]:
    """Parse a CIF file into a flat dictionary representation.

    Parameters
    ----------
    file
        CIF file to be parsed.
    encoding
        Encoding used to decode the file if it is provided as bytes or Path.

    Returns
    -------
    tuple[CIFFlatDict, list[CIFParsingError]]
        A tuple containing the parsed CIF file as a flat dictionary
        and a list of parsing errors encountered during parsing.
    """
    file_content: str = filelike_to_str(file, encoding=encoding)
    parser = CIFParser(file_content)
    return parser.output, parser.errors
