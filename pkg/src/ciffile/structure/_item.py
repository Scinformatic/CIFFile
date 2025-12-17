from typing import Iterator

import polars as pl


class CIFDataItem:

    def __init__(
        self,
        code: str,
        content: pl.Series,
    ):
        self._code = code
        self._values = content
        return

    @property
    def code(self) -> str:
        """Data item (full) name."""
        return self._code

    @property
    def values(self) -> pl.Series:
        """Data item values."""
        return self._values

    @property
    def value(self) -> pl.Series | str | int | float | bool | None:
        """Data item single value.

        If the data item contains multiple values, a Polars Series is returned.
        If the data item contains a single value, that value is returned directly.
        If the data item contains no values, `None` is returned.
        """
        if self._values.is_empty():
            return None
        if len(self._values) == 1:
            return self._values[0]
        return self._values

    def __iter__(self) -> Iterator[str | int | float | bool | None]:
        """Iterate over values in the data item."""
        for value in self._values:
            yield value

    def __getitem__(self, index: int) -> pl.Series:
        """Get a value by its index."""
        return self._values[index]

    def __contains__(self, value: str | int | float | bool | None) -> bool:
        """Check if a value exists in the data item."""
        return value in self._values

    def __len__(self) -> int:
        """Number of values in the data item."""
        return len(self._values)

    def __repr__(self) -> str:
        """String representation of the CIF data item."""
        return f"CIFDataItem(code={self._code!r}, values={len(self)})"
