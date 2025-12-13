"""CIF parser output."""


from typing import TypedDict


class CIFFlatDict(TypedDict):
    """Flat dictionary representation of a CIF file.

    Each key corresponds to a column in a table,
    where each row corresponds to a unique data item in the CIF file.

    Attributes
    ----------
    block_code
        List of block codes (i.e., data block names) for each data item.
        This is the top-level grouping in a CIF file,
        where each data item belongs to a specific data block.
    frame_code
        List of frame codes (i.e., save frame names) for each data item.
        This is the second-level grouping in a CIF file,
        where data items are either directly under a data block
        or belong to a specific save frame within a data block.
        For data items not in a save frame, this value is `None`.
    loop_code
        List of loop codes for each data item.
        A loop code of `0` indicates that the data item is a single item
        (i.e., not part of a looped table).
        A positive loop code (1, 2, ...) indicates that the data item
        is part of a looped table, with the same loop code
        shared among all data items in that table.
    data_name
        List of data names (i.e., data item names) for each data item.
    data_values
        List of data values for each data item.
        Each data value is represented as a list of strings.
        For single data items, this list will contain a single string.
        For tabular (looped) data items, this list will contain multiple strings,
        corresponding to row values for that data item column in the table.
    """

    block_code: list[str]
    frame_code: list[str | None]
    loop_code: list[int]
    data_name: list[str]
    data_values: list[list[str]]
