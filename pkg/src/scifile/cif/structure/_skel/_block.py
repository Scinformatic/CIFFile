"""CIF block data structure base class."""

from scifile.typing import DataFrameLike

from .._util import validate_content_df
from ._base import CIFSkeleton


class CIFBlockSkeleton(CIFSkeleton):
    """CIF block data structure base class."""

    def __init__(
        self,
        *,
        content: DataFrameLike,
        validate: bool,
        require_block: bool,
        require_frame: bool,
        col_name_block: str | None,
        col_name_frame: str | None,
        col_name_cat: str,
        col_name_key: str,
        col_name_values: str,
        **kwargs,
    ) -> None:
        if validate:
            content = validate_content_df(
                content,
                require_block=require_block,
                require_frame=require_frame,
                col_name_block=col_name_block,
                col_name_frame=col_name_frame,
                col_name_cat=col_name_cat,
                col_name_key=col_name_key,
                col_name_values=col_name_values,
            )

        super().__init__(content=content, **kwargs)

        self._col_block = col_name_block
        self._col_frame = col_name_frame
        self._col_cat = col_name_cat
        self._col_key = col_name_key
        self._col_values = col_name_values
        return
