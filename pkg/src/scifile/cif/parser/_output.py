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
    frame_code_category
        List of frame code categories (i.e., first part of save frame names) for each data item.
        This is the second-level grouping in a CIF file,
        where data items are either directly under a data block
        or belong to a specific save frame within a data block.
        For data items not in a save frame, this value is `None`.
        Note that this implementation breaks the frame code into two parts:
        - category: anything before the fist period (.)
        - keyword: anything after the first period (.)
    frame_code_keyword
        List of frame code keywords (i.e., second part of save frame names) for each data item.
        For data items not in a save frame, or frame codes without a period, this value is `None`.
        See `frame_code_category` for more details.
    data_name_category
        List of data name categories (i.e., first part of data item names) for each data item.
        Note that this implementation breaks the data name into two parts:
        - category: anything before the first period (.)
        - keyword: anything after the first period (.)
    data_name_keyword
        List of data name keywords (i.e., second part of data item names) for each data item.
        For data names without a period, this value is `None`.
        See `data_name_category` for more details.
    data_values
        List of data values for each data item.
        Each data value is represented as a list of strings.
        For single data items, this list will contain a single string.
        For tabular (looped) data items, this list will contain multiple strings,
        corresponding to row values for that data item column in the table.
    loop_id
        List of loop IDs for each data item.
        A loop ID of `0` indicates that the data item is a single item
        (i.e., not part of a looped table).
        A positive loop ID (1, 2, ...) indicates that the data item
        is part of a looped table, with the same loop ID
        shared among all data items in that table.
    """

    block_code: list[str]
    frame_code_category: list[str | None]
    frame_code_keyword: list[str | None]
    data_name_category: list[str]
    data_name_keyword: list[str]
    data_values: list[list[str]]
    loop_id: list[int]
