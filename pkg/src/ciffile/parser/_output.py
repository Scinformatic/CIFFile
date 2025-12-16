"""CIF parser output."""


from typing import TypedDict
from ciffile.typing import BlockCode, FrameCode, DataCategory, DataKeyword, DataValues


class CIFFlatDict(TypedDict):
    """Flat dictionary representation of a CIF file.

    Each key corresponds to a column in a table,
    where each row corresponds to a unique data item in the CIF file.

    Attributes
    ----------
    block
        List of block codes (i.e., data block names) for each data item.
    frame
        List of frame codes (i.e., save frame names) for each data item.
    category
        List of data categories for each data item.
    keyword
        List of data keywords for each data item.
    values
        List of data values for each data item.
    """

    block: list[BlockCode]
    frame: list[FrameCode]
    category: list[DataCategory]
    keyword: list[DataKeyword]
    values: list[DataValues]
