"""CIF file validator."""

from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any
    import polars as pl
    from ciffile.structure import CIFFile, CIFBlock, CIFDataCategory


class CIFFileValidator(metaclass=ABCMeta):

    def __init__(self, dictionary: dict) -> None:
        self._dict = dictionary
        return

    @property
    def dict(self) -> dict[str, Any]:
        """Dictionary metadata."""
        return self._dict

    @property
    def dict_title(self) -> str | None:
        """Title of the dictionary."""
        return self._dict["title"]

    @property
    def dict_description(self) -> str | None:
        """Description of the dictionary."""
        return self._dict["description"]

    @property
    def dict_version(self) -> str | None:
        """Version of the dictionary."""
        return self._dict["version"]

    def __getitem__(self, key: str) -> Any:
        return self._dict[key]

    @abstractmethod
    def validate(self, file: CIFFile | CIFBlock | CIFDataCategory, **kwargs) -> pl.DataFrame:
        ...
