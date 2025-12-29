"""CIF file creator."""

from typing import Literal

from .typing import DataFrameLike
from .structure import CIFFile


__all__ = [
    "create",
]


def create(
    content: DataFrameLike,
    *,
    variant: Literal["cif1", "mmcif"] = "mmcif",
    validate: bool = True,
    col_name_block: str = "block",
    col_name_frame: str = "frame",
    col_name_cat: str = "category",
    col_name_key: str = "keyword",
    col_name_values: str = "values",
    allow_duplicate_rows: bool = False,
) -> CIFFile:
    """Create a new CIF file from table-like content.

    Parameters
    ----------
    content
        Content of the CIF file.
        This can be any table-like data structure
        (e.g., a `polars.DataFrame`, `pandas.DataFrame`,
        dictionary of columns, list of rows, etc.)
        that can be converted to a `polars.DataFrame`.
        The resulting DataFrame must contain one row
        for each unique data item in the CIF file,
        with columns specifying:
        - **Block code** (i.e., data block name) of the data item.
        - **Frame code** (i.e., save frame name within the block) of the data item (optional; for CIF dictionary files).
        - **Category** of the data item name (tag).
          For mmCIF files, this corresponds to
          the part before the period in the data name.
          For CIF files, this must be `None` for single data items
          (i.e., not part of a loop/table),
          and a unique value (e.g., "1", "2", ...) for each table,
          shared among all data items in that table.
        - **Keyword** of the data item name (tag).
          For mmCIF files, this corresponds to
          the part after the period in the data name.
          For CIF files, this is the data name itself.
        - **Values** of the data item as a list.
          For single data items, the list contains a single string.
          For tabular (looped) data items,
          it contains multiple strings,
          corresponding to row values
          for that data item column in the table.
    variant
        CIF file variant to create; one of:
        - "cif1": CIF Version 1.1 format.
        - "mmcif": macromolecular CIF format (default)

        This affects validation checks and formatting.
        For example, "mmcif" variants are not allowed
        to have `None` categories, and the full data name
        is constructed as "category.keyword",
        whereas "cif1" variants can have `None` categories,
        and the data name is simply "keyword".
    validate
        Whether to validate the content DataFrame.
        If `True`, checks are performed to ensure
        that the DataFrame conforms to the expected structure
        for the specified CIF variant.
    col_name_block
        Name of the column in `content` that contains
        the block codes. Defaults to "block".
    col_name_frame
        Name of the column in `content` that contains
        the frame codes. Defaults to "frame".
        Note that the presence of the frame column
        is optional.
    col_name_cat
        Name of the column in `content` that contains
        the data categories. Defaults to "category".
    col_name_key
        Name of the column in `content` that contains
        the data keywords. Defaults to "keyword".
    col_name_values
        Name of the column in `content` that contains
        the data values. Defaults to "values".
    allow_duplicate_rows
        Whether to permit duplicate rows (same block, frame, category, key)
        and aggregate them during validation. Defaults to False.

    Returns
    -------
    cif_file
        New `CIFFile` object representing the CIF data.
    """
    return CIFFile(
        content=content,
        variant=variant,
        validate=validate,
        col_name_block=col_name_block,
        col_name_frame=col_name_frame,
        col_name_cat=col_name_cat,
        col_name_key=col_name_key,
        col_name_values=col_name_values,
        allow_duplicate_rows=allow_duplicate_rows,
    )
