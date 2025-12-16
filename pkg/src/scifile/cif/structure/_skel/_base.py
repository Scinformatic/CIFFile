"""CIF data structure base class."""

from abc import abstractmethod, ABCMeta
from typing import Callable, Literal

import polars as pl

from scifile.typing import DataFrameLike


class CIFSkeleton(metaclass=ABCMeta):
    """CIF data structure base class."""

    def __init__(
        self,
        *,
        content: DataFrameLike,
        variant: Literal["cif1", "mmcif"],
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)
        self._df = content if isinstance(content, pl.DataFrame) else pl.DataFrame(content)
        self._variant: Literal["cif1", "mmcif"] = variant
        return

    @abstractmethod
    def write(
        self,
        writer: Callable[[str], None],
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
