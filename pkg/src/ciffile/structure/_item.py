"""CIF data item."""

import polars as pl

from ._base import CIFStructure


class CIFDataItem(CIFStructure[str | int | float | bool | None]):
    """CIF file data item."""

    def __init__(
        self,
        code: str,
        name: str,
        content: pl.Series,
    ):
        super().__init__(code=code, container_type="item")
        self._name = name
        self._values = content
        self._description: str | None = None
        self._unit: str | None = None
        return

    @property
    def name(self) -> str:
        """Data item full name."""
        return self._name

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

    @property
    def description(self) -> str | None:
        """Description of this data category, if available."""
        return self._description

    @description.setter
    def description(self, desc: str | None) -> None:
        """Set the description of this data category."""
        self._description = desc
        return

    @property
    def unit(self) -> str | None:
        """Unit of this data item, if available."""
        return self._unit

    @unit.setter
    def unit(self, unit: str | None) -> None:
        """Set the unit of this data item."""
        self._unit = unit
        return

    def __repr__(self) -> str:
        """String representation of the CIF data item."""
        return f"CIFDataItem(code={self._code!r}, values={len(self)})"

    def _get_codes(self) -> list[str]:
        """Get codes of the data values in this data item."""
        return [str(i) for i in range(len(self._values))]

    def _get_elements(self) -> dict[str, str | int | float | bool | None]:
        """Generate data values for this data item."""
        return {str(i): self._values[i] for i in range(len(self._values))}

    def _get_empty_element(self) -> None:
        return None
