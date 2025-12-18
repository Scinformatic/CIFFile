"""CIF file parser."""

from typing import Literal

from ciffile.typing import FileLike

from ._parser import CIFParser
from ._output import CIFFlatDict
from ._exception import CIFFileParseError

__all__ = [
    "CIFFlatDict",
    "CIFFileParseError",
    "parse",
]


def parse(
    file: FileLike,
    *,
    variant: Literal["cif1", "mmcif"] = "mmcif",
    encoding: str = "utf-8",
    case_normalization: Literal["lower", "upper"] | None = "lower",
    raise_level: Literal[0, 1, 2] = 2,
) -> tuple[CIFFlatDict, list[CIFFileParseError]]:
    """Parse a CIF file into a flat dictionary representation.

    Parameters
    ----------
    file
        CIF file to be parsed.
    variant
        Variant of the CIF format; one of:
        - "cif1": CIF version 1.0 or 1.1
        - "mmcif": macromolecular CIF (default)
    encoding
        Encoding used to decode the file if it is provided as bytes or Path.
    case_normalization
        Case normalization to apply
        to data names and block/frame codes (which are case-insensitive);
        one of:
        - "lower": convert to lowercase (default)
        - "upper": convert to uppercase
        - `None`: no case normalization

    Returns
    -------
    tuple[CIFFlatDict, list[CIFParsingError]]
        A tuple containing the parsed CIF file as a flat dictionary
        and a list of parsing errors encountered during parsing.
    """
    parser = CIFParser(
        file,
        variant=variant,
        encoding=encoding,
        case_normalization=case_normalization,
        raise_level=raise_level
    )
    return parser.output, parser.errors
