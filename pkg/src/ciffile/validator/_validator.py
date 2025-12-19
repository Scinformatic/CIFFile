"""CIF file validator."""

from typing import Any


class CIFFileValidator:

    def __init__(self, dictionary: dict) -> None:
        self._dict = dictionary
        return

    @property
    def dict(self) -> dict[str, Any]:
        """Dictionary metadata."""
        return self._dict
