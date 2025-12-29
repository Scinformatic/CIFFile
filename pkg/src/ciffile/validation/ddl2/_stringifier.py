"""DDL2 data type reverse casting: typed values to CIF strings."""

from __future__ import annotations

import math
from typing import Sequence, Literal, NamedTuple

import polars as pl


class StringifyPlan(NamedTuple):
    """Plan for converting a typed column back to string.

    Attributes
    ----------
    expr
        Polars expression to perform the stringification.
    output_name
        Name of the output column.
    consumes
        Names of columns consumed by this plan.
    """

    expr: pl.Expr
    output_name: str
    consumes: tuple[str, ...]


class Stringifier:
    """Reverse caster: convert DDL2-typed Polars columns back to CIF string format.

    This class reverses the transformations performed by the `Caster` class,
    converting typed Polars columns back to their original CIF string representations.

    Parameters
    ----------
    esd_col_suffix
        Suffix used for estimated standard deviation (ESD) columns.
        ESD columns will be merged back into the main column using parenthesized notation.
    bool_true
        String to use for True values when converting boolean columns.
    bool_false
        String to use for False values when converting boolean columns.
    date_format
        Format string for date output (strftime format).
    datetime_format
        Format string for datetime output (strftime format).
    list_delimiter
        Delimiter used when joining list elements back to string.
    nan_string
        String to use for NaN values (typically ".").
    null_string
        String to use for inapplicable/null values that should become ".".
        When None, null values remain null in the output.
    """

    def __init__(
        self,
        *,
        esd_col_suffix: str = "_esd_digits",
        bool_true: str = "YES",
        bool_false: str = "NO",
        date_format: str = "%Y-%m-%d",
        datetime_format: str = "%Y-%m-%d:%H:%M",
        list_delimiter: str = ",",
        nan_string: str = ".",
        null_to_dot: bool = False,
    ) -> None:
        self._esd_col_suffix = esd_col_suffix
        self._bool_true = bool_true
        self._bool_false = bool_false
        self._date_format = date_format
        self._datetime_format = datetime_format
        self._list_delimiter = list_delimiter
        self._nan_string = nan_string
        self._null_to_dot = null_to_dot

    def stringify_column(
        self,
        df: pl.DataFrame,
        col_name: str,
        *,
        type_code: str | None = None,
        esd_col_name: str | None = None,
        enum_values: list[str] | None = None,
        is_bool_enum: bool = False,
    ) -> StringifyPlan:
        """Generate a plan to stringify a column back to CIF string format.

        Parameters
        ----------
        df
            DataFrame containing the column.
        col_name
            Name of the column to stringify.
        type_code
            DDL2 type code (e.g., "float", "int", "yyyy-mm-dd").
            If None, type is inferred from the Polars dtype.
        esd_col_name
            Name of the ESD column to merge, if any.
        enum_values
            Original enumeration values, if the column was an enum.
        is_bool_enum
            Whether the column is a boolean-like enum (converted from enum to bool).

        Returns
        -------
        StringifyPlan
            Plan for converting the column back to string.
        """
        col = pl.col(col_name)
        dtype = df.schema[col_name]

        consumes = [col_name]
        if esd_col_name and esd_col_name in df.columns:
            consumes.append(esd_col_name)

        # Handle ESD merging for float columns
        if esd_col_name and esd_col_name in df.columns:
            expr = self._merge_float_with_esd(col_name, esd_col_name, dtype)
        # Boolean columns (from enum_to_bool)
        elif dtype == pl.Boolean:
            expr = self._boolean_to_str(col)
        # Enum columns
        elif isinstance(dtype, pl.Enum):
            expr = self._enum_to_str(col)
        # Date/Datetime columns
        elif dtype == pl.Date:
            expr = self._date_to_str(col)
        elif isinstance(dtype, pl.Datetime):
            expr = self._datetime_to_str(col)
        # List columns
        elif isinstance(dtype, pl.List):
            expr = self._list_to_str(col, dtype.inner)
        # Array columns
        elif isinstance(dtype, pl.Array):
            expr = self._array_to_str(col, dtype)
        # Numeric columns
        elif dtype.is_float():
            expr = self._float_to_str(col)
        elif dtype.is_integer():
            expr = self._int_to_str(col)
        # String columns (no-op, but handle normalization)
        elif dtype == pl.Utf8:
            expr = col
        else:
            # Default: cast to string
            expr = col.cast(pl.Utf8)

        if self._null_to_dot:
            expr = expr.fill_null(self._nan_string)

        return StringifyPlan(
            expr=expr.alias(col_name),
            output_name=col_name,
            consumes=tuple(consumes),
        )

    def _boolean_to_str(self, col: pl.Expr) -> pl.Expr:
        """Convert boolean column to string."""
        return (
            pl.when(col.is_null())
            .then(None)
            .when(col)
            .then(pl.lit(self._bool_true))
            .otherwise(pl.lit(self._bool_false))
        )

    def _enum_to_str(self, col: pl.Expr) -> pl.Expr:
        """Convert Enum column back to plain string."""
        # Empty string category ("") becomes null
        return (
            pl.when(col.cast(pl.Utf8) == "")
            .then(None)
            .otherwise(col.cast(pl.Utf8))
        )

    def _date_to_str(self, col: pl.Expr) -> pl.Expr:
        """Convert Date column to string in CIF format."""
        return col.dt.strftime(self._date_format)

    def _datetime_to_str(self, col: pl.Expr) -> pl.Expr:
        """Convert Datetime column to string in CIF format."""
        return col.dt.strftime(self._datetime_format)

    def _list_to_str(self, col: pl.Expr, inner_dtype: pl.DataType) -> pl.Expr:
        """Convert List column to delimited string."""
        # For list of strings or numbers, join with delimiter
        # Empty list becomes "."
        return (
            pl.when(col.is_null())
            .then(None)
            .when(col.list.len() == 0)
            .then(pl.lit(self._nan_string))
            .otherwise(
                col.list.eval(pl.element().cast(pl.Utf8))
                .list.join(self._list_delimiter)
            )
        )

    def _array_to_str(self, col: pl.Expr, dtype: pl.Array) -> pl.Expr:
        """Convert Array column to range string format.

        For 2-element arrays (ranges), outputs "min-max" format.
        """
        # For int-range or float-range (2-element arrays)
        if dtype.size == 2:
            # Extract elements and format as "min-max"
            first = col.arr.get(0).cast(pl.Utf8)
            second = col.arr.get(1).cast(pl.Utf8)
            return (
                pl.when(col.is_null())
                .then(None)
                # Check if it's all NaN (for float) -> "."
                .when(
                    col.arr.get(0).is_nan() & col.arr.get(1).is_nan()
                    if dtype.inner.is_float()
                    else pl.lit(False)
                )
                .then(pl.lit(self._nan_string))
                # If both elements are the same, output single value
                .when(first == second)
                .then(first)
                # Otherwise output "min-max"
                .otherwise(pl.concat_str([first, pl.lit("-"), second]))
            )
        # For other arrays, join with comma
        return col.cast(pl.List(dtype.inner)).list.join(self._list_delimiter)

    def _float_to_str(self, col: pl.Expr) -> pl.Expr:
        """Convert float column to string, handling NaN."""
        return (
            pl.when(col.is_null())
            .then(None)
            .when(col.is_nan())
            .then(pl.lit(self._nan_string))
            .otherwise(col.cast(pl.Utf8))
        )

    def _int_to_str(self, col: pl.Expr) -> pl.Expr:
        """Convert integer column to string."""
        return col.cast(pl.Utf8)

    def _merge_float_with_esd(
        self,
        main_col: str,
        esd_col: str,
        dtype: pl.DataType,
    ) -> pl.Expr:
        """Merge float column with its ESD column back to parenthesized notation.

        Converts float value + integer ESD digits back to "value(esd)" string format.
        For example: 1.234 + 5 -> "1.234(5)"

        The ESD digits represent uncertainty in the last digits of the mantissa.
        To reconstruct the original string:
        1. Format the float value
        2. If ESD is not null, append "(<esd_digits>)"
        """
        main = pl.col(main_col)
        esd = pl.col(esd_col)

        # Handle arrays (float-range)
        if isinstance(dtype, pl.Array):
            return self._merge_float_array_with_esd(main_col, esd_col, dtype)

        # Format float to string
        float_str = (
            pl.when(main.is_null())
            .then(None)
            .when(main.is_nan())
            .then(pl.lit(self._nan_string))
            .otherwise(main.cast(pl.Utf8))
        )

        # Add ESD in parentheses if present
        return (
            pl.when(esd.is_null())
            .then(float_str)
            .otherwise(
                pl.concat_str([
                    float_str,
                    pl.lit("("),
                    esd.cast(pl.Utf8),
                    pl.lit(")"),
                ])
            )
        )

    def _merge_float_array_with_esd(
        self,
        main_col: str,
        esd_col: str,
        dtype: pl.Array,
    ) -> pl.Expr:
        """Merge float array with ESD array back to range string.

        For float-range: converts [1.234, 5.678] + [5, 10] -> "1.234(5)-5.678(10)"
        """
        main = pl.col(main_col)
        esd = pl.col(esd_col)

        def format_element(val: pl.Expr, unc: pl.Expr) -> pl.Expr:
            """Format a single value with optional uncertainty."""
            val_str = val.cast(pl.Utf8)
            return (
                pl.when(unc.is_null())
                .then(val_str)
                .otherwise(
                    pl.concat_str([
                        val_str,
                        pl.lit("("),
                        unc.cast(pl.Utf8),
                        pl.lit(")"),
                    ])
                )
            )

        first_val = main.arr.get(0)
        second_val = main.arr.get(1)
        first_esd = esd.arr.get(0)
        second_esd = esd.arr.get(1)

        first_str = format_element(first_val, first_esd)
        second_str = format_element(second_val, second_esd)

        return (
            pl.when(main.is_null())
            .then(None)
            .when(first_val.is_nan() & second_val.is_nan())
            .then(pl.lit(self._nan_string))
            # If both values and ESDs are the same, output single value
            .when((first_val == second_val) & (first_esd == second_esd))
            .then(first_str)
            # Otherwise output "val1(esd1)-val2(esd2)"
            .otherwise(pl.concat_str([first_str, pl.lit("-"), second_str]))
        )
