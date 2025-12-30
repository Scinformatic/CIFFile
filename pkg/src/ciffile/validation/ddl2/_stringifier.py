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

    The method dispatch mirrors `Caster`: each DDL2 type code has a corresponding
    method that knows exactly how to reverse that specific transformation.

    Parameters
    ----------
    esd_col_suffix
        Suffix used for estimated standard deviation (ESD) columns.
        ESD columns will be merged back into the main column using parenthesized notation.
    bool_true
        String to use for True values when converting "boolean"-type columns.
    bool_false
        String to use for False values when converting "boolean"-type columns.
    date_format
        Format string for date output (strftime format).
    datetime_format
        Format string for datetime output (strftime format).
    null_str
        Symbol to use for null values in string columns.
    null_float
        Symbol to use for null values in floating-point columns.
    null_int
        Symbol to use for null values in integer columns.
    null_bool
        Symbol to use for null values in boolean columns.
    empty_str
        Symbol to use for empty strings in string columns.
    nan_float
        Symbol to use for NaN values in floating-point columns.
    """

    def __init__(
        self,
        *,
        esd_col_suffix: str = "_esd_digits",
        bool_true: str = "YES",
        bool_false: str = "NO",
        date_format: str = "%Y-%m-%d",
        datetime_format: str = "%Y-%m-%d:%H:%M",
        null_str: Literal[".", "?"] = "?",
        null_float: Literal[".", "?"] = "?",
        null_int: Literal[".", "?"] = "?",
        null_bool: Literal[".", "?"] = "?",
        empty_str: Literal[".", "?"] = ".",
        nan_float: Literal[".", "?"] = ".",
    ) -> None:
        self._esd_col_suffix = esd_col_suffix
        self._bool_true = bool_true
        self._bool_false = bool_false
        self._date_format = date_format
        self._datetime_format = datetime_format
        self._null_str = null_str
        self._null_float = null_float
        self._null_int = null_int
        self._null_bool = null_bool
        self._empty_str = empty_str
        self._nan_float = nan_float

        # Map DDL2 type codes to stringifier methods (mirrors Caster._type_to_caster)
        self._type_to_stringifier = {
            "any": self.any,
            "boolean": self.boolean,
            "date_dep": self.date_dep,
            "entity_id_list": self.entity_id_list,
            "float": self.float,
            "float-range": self.float_range,
            "id_list": self.id_list,
            "id_list_spc": self.id_list_spc,
            "int": self.int,
            "int-range": self.int_range,
            "int_list": self.int_list,
            "seq-one-letter-code": self.seq_one_letter_code,
            "sequence_dep": self.sequence_dep,
            "symmetry_operation": self.symmetry_operation,
            "ucode-alphanum-csv": self.ucode_alphanum_csv,
            "yyyy-mm-dd": self.yyyy_mm_dd,
            "yyyy-mm-dd:hh:mm": self.yyyy_mm_dd_hh_mm,
            "yyyy-mm-dd:hh:mm-flex": self.yyyy_mm_dd_hh_mm_flex,
        }

    def __call__(
        self,
        col: str,
        type_code: str,
        *,
        has_esd: bool = False,
        bool_enum_true: str | None = None,
        bool_enum_false: str | None = None,
    ) -> list[StringifyPlan]:
        """Get a stringification plan for a DDL2 data type.

        Parameters
        ----------
        col
            Column name to stringify.
        type_code
            DDL2 data type name (same as used for Caster).
        has_esd
            Whether this column has an associated ESD column to merge.
        bool_enum_true
            String to use for True values when this is a boolean-like enum column.
            If provided (along with bool_enum_false), uses bool_enum stringification.
        bool_enum_false
            String to use for False values when this is a boolean-like enum column.

        Returns
        -------
        list[StringifyPlan]
            List of stringification plans. Usually one plan that produces the
            output column. For types with ESD columns, consumes both main and ESD.
        """
        # Special handling for boolean-like enums
        if bool_enum_true is not None and bool_enum_false is not None:
            return self.bool_enum(col, bool_enum_true, bool_enum_false)

        # Dispatch to type-specific method
        stringifier = self._type_to_stringifier.get(type_code, self.any)

        # For float types, pass ESD information
        if type_code == "float" and has_esd:
            return self.float_with_esd(col)
        elif type_code == "float-range" and has_esd:
            return self.float_range_with_esd(col)

        return stringifier(col)

    # ========== Type-specific stringifiers ==========

    def any(self, col: str) -> list[StringifyPlan]:
        """Stringify 'any' type: cast to string, null → null_str, empty → empty_str.

        This method handles string columns. For non-string columns (like dates),
        it simply casts to string without empty-string checking.
        """
        c = pl.col(col)
        # Build expression that handles null and casts to string
        # The empty string check is only valid for string types, so we use
        # a post-cast check instead of pre-cast comparison
        expr = (
            pl.when(c.is_null())
            .then(pl.lit(self._null_str))
            .otherwise(
                pl.when(c.cast(pl.Utf8) == "")
                .then(pl.lit(self._empty_str))
                .otherwise(c.cast(pl.Utf8))
            )
            .alias(col)
        )
        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col,),
        )]

    def boolean(self, col: str) -> list[StringifyPlan]:
        """Stringify 'boolean' type using bool_true/bool_false."""
        c = pl.col(col)
        expr = (
            pl.when(c.is_null())
            .then(pl.lit(self._null_bool))
            .when(c)
            .then(pl.lit(self._bool_true))
            .otherwise(pl.lit(self._bool_false))
            .alias(col)
        )
        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col,),
        )]

    def bool_enum(self, col: str, true_val: str, false_val: str) -> list[StringifyPlan]:
        """Stringify boolean-like enum using provided true/false values."""
        c = pl.col(col)
        expr = (
            pl.when(c.is_null())
            .then(pl.lit(self._null_bool))
            .when(c)
            .then(pl.lit(true_val))
            .otherwise(pl.lit(false_val))
            .alias(col)
        )
        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col,),
        )]

    def date_dep(self, col: str) -> list[StringifyPlan]:
        """Stringify 'date_dep' type: date → string."""
        c = pl.col(col)
        expr = (
            pl.when(c.is_null())
            .then(pl.lit(self._null_str))
            .otherwise(c.dt.strftime(self._date_format))
            .alias(col)
        )
        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col,),
        )]

    def entity_id_list(self, col: str) -> list[StringifyPlan]:
        """Stringify 'entity_id_list' type: list → comma-separated."""
        return self._list_to_delimited(col, ",")

    def float(self, col: str) -> list[StringifyPlan]:
        """Stringify 'float' type without ESD: float → string, NaN → nan_float."""
        c = pl.col(col)
        expr = (
            pl.when(c.is_null())
            .then(pl.lit(self._null_float))
            .when(c.is_nan())
            .then(pl.lit(self._nan_float))
            .otherwise(c.cast(pl.Utf8))
            .alias(col)
        )
        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col,),
        )]

    def float_with_esd(self, col: str) -> list[StringifyPlan]:
        """Stringify 'float' type with ESD: merge float + ESD → 'value(esd)'."""
        esd_col = f"{col}{self._esd_col_suffix}"
        main = pl.col(col)
        esd = pl.col(esd_col)

        # Format float to string
        float_str = main.cast(pl.Utf8)

        # Add ESD in parentheses if present
        expr = (
            pl.when(main.is_null())
            .then(pl.lit(self._null_float))
            .when(main.is_nan())
            .then(pl.lit(self._nan_float))
            .when(esd.is_null())
            .then(float_str)
            .otherwise(
                pl.concat_str([
                    float_str,
                    pl.lit("("),
                    esd.cast(pl.Utf8),
                    pl.lit(")"),
                ])
            )
            .alias(col)
        )

        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col, esd_col),
        )]

    def float_range(self, col: str) -> list[StringifyPlan]:
        """Stringify 'float-range' type without ESD: array → 'min-max'."""
        c = pl.col(col)

        first = c.arr.get(0)
        second = c.arr.get(1)
        first_str = first.cast(pl.Utf8)
        second_str = second.cast(pl.Utf8)

        expr = (
            pl.when(c.is_null())
            .then(pl.lit(self._null_float))
            .when(first.is_nan() & second.is_nan())
            .then(pl.lit(self._nan_float))
            # If both elements are the same, output single value
            .when(first == second)
            .then(first_str)
            # Otherwise output "min-max"
            .otherwise(pl.concat_str([first_str, pl.lit("-"), second_str]))
            .alias(col)
        )

        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col,),
        )]

    def float_range_with_esd(self, col: str) -> list[StringifyPlan]:
        """Stringify 'float-range' type with ESD: arrays → 'val1(esd1)-val2(esd2)'."""
        esd_col = f"{col}{self._esd_col_suffix}"
        main = pl.col(col)
        esd = pl.col(esd_col)

        first_val = main.arr.get(0)
        second_val = main.arr.get(1)
        first_esd = esd.arr.get(0)
        second_esd = esd.arr.get(1)

        # Format single element with optional ESD
        def format_element(val: pl.Expr, unc: pl.Expr) -> pl.Expr:
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

        first_str = format_element(first_val, first_esd)
        second_str = format_element(second_val, second_esd)

        expr = (
            pl.when(main.is_null())
            .then(pl.lit(self._null_float))
            .when(first_val.is_nan() & second_val.is_nan())
            .then(pl.lit(self._nan_float))
            # If both values and ESDs are the same, output single value
            .when(
                (first_val == second_val) &
                ((first_esd.is_null() & second_esd.is_null()) | (first_esd == second_esd))
            )
            .then(first_str)
            # Otherwise output "val1(esd1)-val2(esd2)"
            .otherwise(pl.concat_str([first_str, pl.lit("-"), second_str]))
            .alias(col)
        )

        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col, esd_col),
        )]

    def id_list(self, col: str) -> list[StringifyPlan]:
        """Stringify 'id_list' type: list → comma-separated."""
        return self._list_to_delimited(col, ",")

    def id_list_spc(self, col: str) -> list[StringifyPlan]:
        """Stringify 'id_list_spc' type: list → space-separated."""
        return self._list_to_delimited(col, " ")

    def int(self, col: str) -> list[StringifyPlan]:
        """Stringify 'int' type: integer → string."""
        c = pl.col(col)
        expr = (
            pl.when(c.is_null())
            .then(pl.lit(self._null_int))
            .otherwise(c.cast(pl.Utf8))
            .alias(col)
        )
        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col,),
        )]

    def int_range(self, col: str) -> list[StringifyPlan]:
        """Stringify 'int-range' type: array → 'min-max'."""
        c = pl.col(col)

        first = c.arr.get(0)
        second = c.arr.get(1)
        first_str = first.cast(pl.Utf8)
        second_str = second.cast(pl.Utf8)

        # Check if both elements are null (for "." case)
        both_null = first.is_null() & second.is_null()

        expr = (
            pl.when(c.is_null())
            .then(pl.lit(self._null_int))
            .when(both_null)
            .then(pl.lit(self._empty_str))
            # If both elements are the same, output single value
            .when(first == second)
            .then(first_str)
            # Otherwise output "min-max"
            .otherwise(pl.concat_str([first_str, pl.lit("-"), second_str]))
            .alias(col)
        )

        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col,),
        )]

    def int_list(self, col: str) -> list[StringifyPlan]:
        """Stringify 'int_list' type: list of integers → comma-separated."""
        return self._list_to_delimited(col, ",")

    def seq_one_letter_code(self, col: str) -> list[StringifyPlan]:
        """Stringify 'seq-one-letter-code' type: string passthrough."""
        return self.any(col)

    def sequence_dep(self, col: str) -> list[StringifyPlan]:
        """Stringify 'sequence_dep' type: string passthrough."""
        return self.any(col)

    def symmetry_operation(self, col: str) -> list[StringifyPlan]:
        """Stringify 'symmetry_operation' type: list → comma-separated."""
        return self._list_to_delimited(col, ",")

    def ucode_alphanum_csv(self, col: str) -> list[StringifyPlan]:
        """Stringify 'ucode-alphanum-csv' type: list → comma-separated."""
        return self._list_to_delimited(col, ",")

    def yyyy_mm_dd(self, col: str) -> list[StringifyPlan]:
        """Stringify 'yyyy-mm-dd' type: date → string."""
        return self.date_dep(col)

    def yyyy_mm_dd_hh_mm(self, col: str) -> list[StringifyPlan]:
        """Stringify 'yyyy-mm-dd:hh:mm' type: datetime → string."""
        c = pl.col(col)
        expr = (
            pl.when(c.is_null())
            .then(pl.lit(self._null_str))
            .otherwise(c.dt.strftime(self._datetime_format))
            .alias(col)
        )
        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col,),
        )]

    def yyyy_mm_dd_hh_mm_flex(self, col: str) -> list[StringifyPlan]:
        """Stringify 'yyyy-mm-dd:hh:mm-flex' type: datetime → string."""
        return self.yyyy_mm_dd_hh_mm(col)

    def enum(self, col: str) -> list[StringifyPlan]:
        """Stringify Enum column: convert back to plain string."""
        c = pl.col(col)
        # Empty string category ("") becomes empty_str symbol
        expr = (
            pl.when(c.is_null())
            .then(pl.lit(self._null_str))
            .when(c.cast(pl.Utf8) == "")
            .then(pl.lit(self._empty_str))
            .otherwise(c.cast(pl.Utf8))
            .alias(col)
        )
        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col,),
        )]

    # ========== Helper methods ==========

    def _list_to_delimited(self, col: str, delimiter: str) -> list[StringifyPlan]:
        """Convert List column to delimited string."""
        c = pl.col(col)
        expr = (
            pl.when(c.is_null())
            .then(pl.lit(self._null_str))
            .when(c.list.len() == 0)
            .then(pl.lit(self._empty_str))
            .otherwise(
                c.list.eval(pl.element().cast(pl.Utf8))
                .list.join(delimiter)
            )
            .alias(col)
        )
        return [StringifyPlan(
            expr=expr,
            output_name=col,
            consumes=(col,),
        )]


def pick_bool_enum_pair(
    enum_values: Sequence[str],
    enum_true: set[str],
    enum_false: set[str],
) -> tuple[str, str] | None:
    """Pick a consistent pair of truthy/falsy values from an enumeration.

    Given an enumeration and sets of truthy/falsy indicators (lowercase),
    returns a consistent pair of (true_value, false_value) from the original
    enumeration values.

    A "consistent" pair means values that match in pattern/length, e.g.,
    ("yes", "no") or ("y", "n") or ("Yes", "No"), not ("yes", "n").

    Parameters
    ----------
    enum_values
        Original enumeration values (case-sensitive).
    enum_true
        Set of lowercase strings indicating truthy values.
    enum_false
        Set of lowercase strings indicating falsy values.

    Returns
    -------
    tuple[str, str] | None
        (truthy_value, falsy_value) pair, or None if no valid pair found.
    """
    # Separate enum values into truthy and falsy groups
    truthy_vals: list[str] = []
    falsy_vals: list[str] = []

    for val in enum_values:
        val_lower = val.lower()
        if val_lower in enum_true:
            truthy_vals.append(val)
        elif val_lower in enum_false:
            falsy_vals.append(val)

    if not truthy_vals or not falsy_vals:
        return None

    # Try to find a consistent pair by matching lowercase patterns
    # Group by lowercase value
    truthy_by_lower: dict[str, list[str]] = {}
    for val in truthy_vals:
        truthy_by_lower.setdefault(val.lower(), []).append(val)

    falsy_by_lower: dict[str, list[str]] = {}
    for val in falsy_vals:
        falsy_by_lower.setdefault(val.lower(), []).append(val)

    # Look for matching pairs by length and case pattern
    # e.g., "yes"/"no", "y"/"n", "Yes"/"No"
    for t_lower, t_vals in truthy_by_lower.items():
        for f_lower, f_vals in falsy_by_lower.items():
            # Check if they match in length
            if len(t_lower) == len(f_lower):
                # Pick first value from each that matches case pattern
                for t_val in t_vals:
                    for f_val in f_vals:
                        # Check case pattern match (both lowercase, both title, etc.)
                        if (t_val.islower() == f_val.islower() and
                            t_val.isupper() == f_val.isupper() and
                            t_val.istitle() == f_val.istitle()):
                            return (t_val, f_val)

    # Fallback: try to find any pair with same length
    for t_val in truthy_vals:
        for f_val in falsy_vals:
            if len(t_val) == len(f_val):
                return (t_val, f_val)

    # Last resort: just pick the first from each
    return (truthy_vals[0], falsy_vals[0])

