"""CIF data structure base classes."""

from abc import abstractmethod, ABCMeta
from typing import Callable, Literal

import polars as pl

from scifile.typing import DataFrameLike

from ._util import validate_content_df


class CIFSkeleton(metaclass=ABCMeta):
    """CIF data structure base class."""

    def __init__(
        self,
        *,
        content: DataFrameLike,
        variant: Literal["cif1", "mmcif"],
        **_: object,
    ) -> None:
        self._df = content if isinstance(content, pl.DataFrame) else pl.DataFrame(content)
        self._variant: Literal["cif1", "mmcif"] = variant
        return

    @abstractmethod
    def write(
        self,
        writer: Callable[[str], None],
        *,
        **kwargs,
    ) -> None:
        """Write the CIF data structure to the writer."""
        raise NotImplementedError("Subclasses must implement the write method.")

    @property
    def df(self) -> pl.DataFrame:
        """DataFrame representation of the CIF data structure."""
        return self._df

    def __str__(self) -> str:
        """String representation of the CIF data structure."""
        chunks = []
        self.write(chunks.append)
        return "".join(chunks)


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


class CIFFileSkeleton(CIFBlockSkeleton):
    """CIF file data structure base class."""

    def __init__(
        self,
        *,
        col_name_frame: str | None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if col_name_frame in self.df.columns:
            if self.df.select(pl.col(col_name_frame).is_null().all()).item():
                self._df = self.df.drop(col_name_frame)
                col_name_frame = None
                filetype = "data"
            else:
                filetype = "dict"
        else:
            col_name_frame = None
            filetype = "data"

        self._col_frame = col_name_frame
        self._filetype = filetype
        return

    @property
    def type(self) -> Literal["data", "dict"]:
        """Type of the CIF file/block.

        Either:
        - "data": File contains no save frames; all data items are directly under data blocks.
        - "dict": File contains save frames; some data items are under save frames.
        """
        return self._filetype
