from typing import Literal

from ._block import CIFBlock
from ._category import CIFDataCategory
from ._file import CIFFile
from ._frame import CIFFrame


from scifile.typing import DataFrameLike


__all__ = [
    "CIFBlock",
    "CIFDataCategory",
    "CIFFile",
    "CIFFrame",
]


def file(
    content: DataFrameLike,
    *,
    variant: Literal["cif1", "mmcif"] = "mmcif",
    validate: bool = True,
    col_name_block: str = "block",
    col_name_frame: str = "frame",
    col_name_cat: str = "category",
    col_name_key: str = "keyword",
    col_name_values: str = "values",
) -> CIFFile:
    return CIFFile(
        content=content,
        variant=variant,
        validate=validate,
        col_name_block=col_name_block,
        col_name_frame=col_name_frame,
        col_name_cat=col_name_cat,
        col_name_key=col_name_key,
        col_name_values=col_name_values,
    )
