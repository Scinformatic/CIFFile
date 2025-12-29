"""Tests for DDL2 values_to_str reverse casting functionality.

Tests that validate() followed by values_to_str() produces the original string values.
"""

import pytest
import polars as pl

from ciffile.structure import CIFDataCategory
from ciffile.validation.ddl2 import DDL2Validator
from ciffile.validation.ddl2._stringifier import Stringifier, pick_bool_enum_pair


class TestPickBoolEnumPair:
    """Tests for the pick_bool_enum_pair helper function."""

    def test_simple_matching_pairs(self) -> None:
        """Test picking from simple matching pairs."""
        enum_true = {"yes", "y", "true"}
        enum_false = {"no", "n", "false"}
        result = pick_bool_enum_pair(["yes", "no", "y", "n"], enum_true, enum_false)
        assert result in [("yes", "no"), ("y", "n")]

    def test_mixed_case_title(self) -> None:
        """Test picking consistent title case pairs."""
        enum_true = {"yes", "y", "true"}
        enum_false = {"no", "n", "false"}
        result = pick_bool_enum_pair(["Yes", "No", "yes", "no"], enum_true, enum_false)
        assert result in [("Yes", "No"), ("yes", "no")]

    def test_only_short_forms(self) -> None:
        """Test with only short form values."""
        enum_true = {"yes", "y", "true"}
        enum_false = {"no", "n", "false"}
        result = pick_bool_enum_pair(["y", "n"], enum_true, enum_false)
        assert result == ("y", "n")

    def test_only_long_forms(self) -> None:
        """Test with only long form values."""
        enum_true = {"yes", "y", "true"}
        enum_false = {"no", "n", "false"}
        result = pick_bool_enum_pair(["yes", "no"], enum_true, enum_false)
        assert result == ("yes", "no")

    def test_all_uppercase(self) -> None:
        """Test with all uppercase values."""
        enum_true = {"yes", "y", "true"}
        enum_false = {"no", "n", "false"}
        result = pick_bool_enum_pair(["YES", "NO"], enum_true, enum_false)
        assert result == ("YES", "NO")

    def test_mixed_lengths_picks_matching(self) -> None:
        """Test that mixed lengths picks matching length pairs."""
        enum_true = {"yes", "y", "true"}
        enum_false = {"no", "n", "false"}
        result = pick_bool_enum_pair(["yes", "n", "y", "no"], enum_true, enum_false)
        assert result in [("yes", "no"), ("y", "n")]
        assert len(result[0]) == len(result[1])

    def test_true_false_variants(self) -> None:
        """Test with true/false string variants."""
        enum_true = {"yes", "y", "true"}
        enum_false = {"no", "n", "false"}
        result = pick_bool_enum_pair(["true", "false"], enum_true, enum_false)
        assert result == ("true", "false")

    def test_no_truthy_returns_none(self) -> None:
        """Test that missing truthy values returns None."""
        enum_true = {"yes", "y", "true"}
        enum_false = {"no", "n", "false"}
        result = pick_bool_enum_pair(["no", "n"], enum_true, enum_false)
        assert result is None

    def test_no_falsy_returns_none(self) -> None:
        """Test that missing falsy values returns None."""
        enum_true = {"yes", "y", "true"}
        enum_false = {"no", "n", "false"}
        result = pick_bool_enum_pair(["yes", "y"], enum_true, enum_false)
        assert result is None

    def test_complex_mix_consistent_case(self) -> None:
        """Test complex mix picks consistent case pattern."""
        enum_true = {"yes", "y", "true"}
        enum_false = {"no", "n", "false"}
        result = pick_bool_enum_pair(
            ["Y", "N", "yes", "no", "Yes", "No"], enum_true, enum_false
        )
        assert result is not None
        assert result[0].lower() in enum_true
        assert result[1].lower() in enum_false
        assert len(result[0]) == len(result[1])


class TestStringifierTypeDispatch:
    """Tests for Stringifier type-code-based dispatch."""

    @pytest.fixture
    def stringifier(self) -> Stringifier:
        """Create a default Stringifier instance."""
        return Stringifier()

    def test_boolean_type(self, stringifier: Stringifier) -> None:
        """Test 'boolean' type uses bool_true/bool_false."""
        df = pl.DataFrame({"col": [True, False, None]})
        plans = stringifier("col", "boolean")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["YES", "NO", None]

    def test_boolean_with_custom_values(self) -> None:
        """Test 'boolean' type with custom true/false strings."""
        stringifier = Stringifier(bool_true="TRUE", bool_false="FALSE")
        df = pl.DataFrame({"col": [True, False, None]})
        plans = stringifier("col", "boolean")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["TRUE", "FALSE", None]

    def test_bool_enum(self, stringifier: Stringifier) -> None:
        """Test boolean-like enum with custom values."""
        df = pl.DataFrame({"col": [True, False, None]})
        plans = stringifier("col", "any", bool_enum_true="y", bool_enum_false="n")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["y", "n", None]

    def test_int_type(self, stringifier: Stringifier) -> None:
        """Test 'int' type converts to string."""
        df = pl.DataFrame({"col": [1, 2, -3, None]})
        plans = stringifier("col", "int")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1", "2", "-3", None]

    def test_float_type_without_esd(self, stringifier: Stringifier) -> None:
        """Test 'float' type without ESD."""
        df = pl.DataFrame({"col": [1.234, float("nan"), None]})
        plans = stringifier("col", "float", has_esd=False)
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1.234", ".", None]

    def test_float_type_with_esd(self, stringifier: Stringifier) -> None:
        """Test 'float' type with ESD merging."""
        df = pl.DataFrame({
            "col": [1.234, 5.678, 9.0, None],
            "col_esd_digits": [5, None, 10, 3],
        })
        plans = stringifier("col", "float", has_esd=True)
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1.234(5)", "5.678", "9.0(10)", None]

    def test_int_range_type(self, stringifier: Stringifier) -> None:
        """Test 'int-range' type formats as 'min-max'."""
        df = pl.DataFrame({
            "col": pl.Series([[1, 5], [3, 3], [None, None], None]).cast(
                pl.Array(pl.Int64, 2)
            )
        })
        plans = stringifier("col", "int-range")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1-5", "3", ".", None]

    def test_id_list_comma_separated(self, stringifier: Stringifier) -> None:
        """Test 'id_list' type produces comma-separated strings."""
        df = pl.DataFrame({"col": [["a", "b", "c"], ["x"], []]})
        plans = stringifier("col", "id_list")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["a,b,c", "x", "."]

    def test_id_list_spc_space_separated(self, stringifier: Stringifier) -> None:
        """Test 'id_list_spc' type produces space-separated strings."""
        df = pl.DataFrame({"col": [["a", "b", "c"], ["x"], []]})
        plans = stringifier("col", "id_list_spc")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["a b c", "x", "."]

    def test_date_type(self, stringifier: Stringifier) -> None:
        """Test 'yyyy-mm-dd' type formats dates."""
        df = pl.DataFrame({
            "col": pl.Series(["2023-01-15", "2024-06-01"]).str.strptime(
                pl.Date, "%Y-%m-%d"
            )
        })
        plans = stringifier("col", "yyyy-mm-dd")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["2023-01-15", "2024-06-01"]

    def test_datetime_type(self, stringifier: Stringifier) -> None:
        """Test 'yyyy-mm-dd:hh:mm' type formats datetimes."""
        df = pl.DataFrame({
            "col": pl.Series(["2023-01-15 10:30", "2024-06-01 14:45"]).str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M"
            )
        })
        plans = stringifier("col", "yyyy-mm-dd:hh:mm")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["2023-01-15:10:30", "2024-06-01:14:45"]

    def test_enum_type(self, stringifier: Stringifier) -> None:
        """Test Enum dtype conversion."""
        df = pl.DataFrame({
            "col": pl.Series(["A", "B", ""]).cast(pl.Enum(["A", "B", ""]))
        })
        plans = stringifier.enum("col")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["A", "B", None]

    def test_any_type_passthrough(self, stringifier: Stringifier) -> None:
        """Test 'any' type passes through strings."""
        df = pl.DataFrame({"col": ["hello", "world", None]})
        plans = stringifier("col", "any")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["hello", "world", None]

    def test_unknown_type_fallback(self, stringifier: Stringifier) -> None:
        """Test unknown types fall back to 'any'."""
        df = pl.DataFrame({"col": ["test", "data", None]})
        plans = stringifier("col", "unknown_type_xyz")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["test", "data", None]

    def test_null_to_dot_option(self) -> None:
        """Test null_to_dot option converts nulls to '.'."""
        stringifier = Stringifier(null_to_dot=True)
        df = pl.DataFrame({"col": [1, 2, None]})
        plans = stringifier("col", "int")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1", "2", "."]


class TestStringifierDirectConversion:
    """Tests for direct Stringifier conversions with pre-typed DataFrames.

    These tests simulate the round-trip by creating typed DataFrames
    (as if produced by validate()) and verifying they convert back correctly.
    """

    @pytest.fixture
    def stringifier(self) -> Stringifier:
        """Create a default Stringifier instance."""
        return Stringifier()

    def test_int_conversion(self, stringifier: Stringifier) -> None:
        """Test int type converts back to original strings."""
        # Simulate post-validate int column
        df = pl.DataFrame({"col": [123, -456, 0, None]})
        plans = stringifier("col", "int")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["123", "-456", "0", None]

    def test_float_conversion_without_esd(self, stringifier: Stringifier) -> None:
        """Test float type without ESD converts to strings."""
        df = pl.DataFrame({"col": [1.234, -5.678, 0.0, None]})
        plans = stringifier("col", "float", has_esd=False)
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1.234", "-5.678", "0.0", None]

    def test_float_conversion_with_esd(self, stringifier: Stringifier) -> None:
        """Test float type with ESD merges correctly."""
        df = pl.DataFrame({
            "col": [1.234, 5.678, 9.0, None],
            "col_esd_digits": [5, None, 10, None],
        })
        plans = stringifier("col", "float", has_esd=True)
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1.234(5)", "5.678", "9.0(10)", None]
        # Check that ESD column is consumed
        consumed = set()
        for plan in plans:
            consumed.update(plan.consumes)
        assert "col_esd_digits" in consumed

    def test_float_nan_to_dot(self, stringifier: Stringifier) -> None:
        """Test float NaN values convert to '.'."""
        import math
        df = pl.DataFrame({"col": [1.234, math.nan, None]})
        plans = stringifier("col", "float", has_esd=False)
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1.234", ".", None]

    def test_boolean_conversion(self, stringifier: Stringifier) -> None:
        """Test boolean type uses YES/NO strings."""
        df = pl.DataFrame({"col": [True, False, None]})
        plans = stringifier("col", "boolean")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["YES", "NO", None]

    def test_date_conversion(self, stringifier: Stringifier) -> None:
        """Test date type formats correctly."""
        df = pl.DataFrame({
            "col": pl.Series(["2023-01-15", "2024-06-01", None]).str.strptime(
                pl.Date, "%Y-%m-%d", strict=False
            )
        })
        plans = stringifier("col", "yyyy-mm-dd")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["2023-01-15", "2024-06-01", None]

    def test_datetime_conversion(self, stringifier: Stringifier) -> None:
        """Test datetime type formats correctly."""
        df = pl.DataFrame({
            "col": pl.Series(["2023-01-15 10:30", "2024-06-01 14:45"]).str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M"
            )
        })
        plans = stringifier("col", "yyyy-mm-dd:hh:mm")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["2023-01-15:10:30", "2024-06-01:14:45"]

    def test_id_list_comma_conversion(self, stringifier: Stringifier) -> None:
        """Test id_list produces comma-separated strings."""
        df = pl.DataFrame({"col": [["a", "b", "c"], ["x"], [], None]})
        plans = stringifier("col", "id_list")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["a,b,c", "x", ".", None]

    def test_id_list_spc_conversion(self, stringifier: Stringifier) -> None:
        """Test id_list_spc produces space-separated strings."""
        df = pl.DataFrame({"col": [["a", "b", "c"], ["x"], [], None]})
        plans = stringifier("col", "id_list_spc")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["a b c", "x", ".", None]

    def test_int_list_conversion(self, stringifier: Stringifier) -> None:
        """Test int_list produces comma-separated strings."""
        df = pl.DataFrame({"col": [[1, 2, 3], [42], [], None]})
        plans = stringifier("col", "int_list")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1,2,3", "42", ".", None]

    def test_int_range_conversion(self, stringifier: Stringifier) -> None:
        """Test int-range type formats as 'min-max'."""
        df = pl.DataFrame({
            "col": pl.Series([[1, 5], [3, 3], [None, None], None]).cast(
                pl.Array(pl.Int64, 2)
            )
        })
        plans = stringifier("col", "int-range")
        result = df.with_columns([p.expr for p in plans])
        # Same min/max produces single value, null array produces "."
        assert result["col"].to_list() == ["1-5", "3", ".", None]

    def test_float_range_conversion(self, stringifier: Stringifier) -> None:
        """Test float-range type formats correctly."""
        df = pl.DataFrame({
            "col": pl.Series([[1.5, 3.5], [2.0, 2.0]]).cast(pl.Array(pl.Float64, 2))
        })
        plans = stringifier("col", "float-range")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1.5-3.5", "2.0"]

    def test_enum_conversion(self, stringifier: Stringifier) -> None:
        """Test Enum dtype converts to strings."""
        df = pl.DataFrame({
            "col": pl.Series(["A", "B", ""]).cast(pl.Enum(["A", "B", ""]))
        })
        plans = stringifier.enum("col")
        result = df.with_columns([p.expr for p in plans])
        # Empty string becomes null
        assert result["col"].to_list() == ["A", "B", None]

    def test_bool_enum_conversion(self, stringifier: Stringifier) -> None:
        """Test boolean-like enum uses specified values."""
        df = pl.DataFrame({"col": [True, False, None]})
        plans = stringifier("col", "any", bool_enum_true="yes", bool_enum_false="no")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["yes", "no", None]

    def test_multiple_columns_conversion(self, stringifier: Stringifier) -> None:
        """Test converting multiple columns of different types."""
        df = pl.DataFrame({
            "int_col": [1, 2, 3],
            "float_col": [1.5, 2.5, 3.5],
            "bool_col": [True, False, True],
        })
        int_plans = stringifier("int_col", "int")
        float_plans = stringifier("float_col", "float", has_esd=False)
        bool_plans = stringifier("bool_col", "boolean")

        all_exprs = [p.expr for p in int_plans + float_plans + bool_plans]
        result = df.with_columns(all_exprs)

        assert result["int_col"].to_list() == ["1", "2", "3"]
        assert result["float_col"].to_list() == ["1.5", "2.5", "3.5"]
        assert result["bool_col"].to_list() == ["YES", "NO", "YES"]

    def test_null_to_dot_all_types(self) -> None:
        """Test null_to_dot option across types."""
        stringifier = Stringifier(null_to_dot=True)

        # Int
        df = pl.DataFrame({"col": [1, None]})
        result = df.with_columns([p.expr for p in stringifier("col", "int")])
        assert result["col"].to_list() == ["1", "."]

        # Float
        df = pl.DataFrame({"col": [1.0, None]})
        result = df.with_columns([p.expr for p in stringifier("col", "float")])
        assert result["col"].to_list() == ["1.0", "."]

        # String
        df = pl.DataFrame({"col": ["a", None]})
        result = df.with_columns([p.expr for p in stringifier("col", "any")])
        assert result["col"].to_list() == ["a", "."]

    def test_preserve_string_passthrough(self, stringifier: Stringifier) -> None:
        """Test 'any' type preserves strings unchanged."""
        df = pl.DataFrame({"col": ["hello", "world", "test", None]})
        plans = stringifier("col", "any")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["hello", "world", "test", None]


class TestStringifierEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_empty_dataframe(self) -> None:
        """Test with empty DataFrame."""
        stringifier = Stringifier()
        df = pl.DataFrame({"col": pl.Series([], dtype=pl.Int64)})
        plans = stringifier("col", "int")
        result = df.with_columns([p.expr for p in plans])
        assert len(result) == 0
        assert result.schema["col"] == pl.Utf8

    def test_all_null_column(self) -> None:
        """Test column with all null values."""
        stringifier = Stringifier()
        df = pl.DataFrame({"col": [None, None, None]})
        plans = stringifier("col", "any")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == [None, None, None]

    def test_float_range_same_values(self) -> None:
        """Test float-range with identical min/max outputs single value."""
        stringifier = Stringifier()
        df = pl.DataFrame({
            "col": pl.Series([[1.5, 1.5], [2.0, 3.0]]).cast(pl.Array(pl.Float64, 2))
        })
        plans = stringifier("col", "float-range")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1.5", "2.0-3.0"]

    def test_float_range_with_nan(self) -> None:
        """Test float-range with NaN values becomes '.'."""
        stringifier = Stringifier()
        import math
        df = pl.DataFrame({
            "col": pl.Series([[math.nan, math.nan], [1.0, 2.0]]).cast(
                pl.Array(pl.Float64, 2)
            )
        })
        plans = stringifier("col", "float-range")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == [".", "1.0-2.0"]

    def test_entity_id_list_type(self) -> None:
        """Test entity_id_list produces comma-separated output."""
        stringifier = Stringifier()
        df = pl.DataFrame({"col": [["A", "B"], ["C"]]})
        plans = stringifier("col", "entity_id_list")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["A,B", "C"]

    def test_symmetry_operation_type(self) -> None:
        """Test symmetry_operation produces comma-separated output."""
        stringifier = Stringifier()
        df = pl.DataFrame({"col": [["x,y,z", "-x,-y,z"], ["x,y,-z"]]})
        plans = stringifier("col", "symmetry_operation")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["x,y,z,-x,-y,z", "x,y,-z"]

    def test_seq_one_letter_code_type(self) -> None:
        """Test seq-one-letter-code passes through."""
        stringifier = Stringifier()
        df = pl.DataFrame({"col": ["ACGT", "MWRK"]})
        plans = stringifier("col", "seq-one-letter-code")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["ACGT", "MWRK"]

    def test_sequence_dep_type(self) -> None:
        """Test sequence_dep passes through."""
        stringifier = Stringifier()
        df = pl.DataFrame({"col": ["SEQUENCE", "DATA"]})
        plans = stringifier("col", "sequence_dep")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["SEQUENCE", "DATA"]

    def test_ucode_alphanum_csv_type(self) -> None:
        """Test ucode-alphanum-csv produces comma-separated output."""
        stringifier = Stringifier()
        df = pl.DataFrame({"col": [["CODE1", "CODE2"], ["SINGLE"]]})
        plans = stringifier("col", "ucode-alphanum-csv")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["CODE1,CODE2", "SINGLE"]

    def test_date_dep_type(self) -> None:
        """Test date_dep type formats dates correctly."""
        stringifier = Stringifier()
        df = pl.DataFrame({
            "col": pl.Series(["2023-05-20"]).str.strptime(pl.Date, "%Y-%m-%d")
        })
        plans = stringifier("col", "date_dep")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["2023-05-20"]

    def test_yyyy_mm_dd_hh_mm_flex_type(self) -> None:
        """Test yyyy-mm-dd:hh:mm-flex type."""
        stringifier = Stringifier()
        df = pl.DataFrame({
            "col": pl.Series(["2023-05-20 15:45"]).str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M"
            )
        })
        plans = stringifier("col", "yyyy-mm-dd:hh:mm-flex")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["2023-05-20:15:45"]

    def test_custom_nan_string(self) -> None:
        """Test custom nan_string option."""
        stringifier = Stringifier(nan_string="N/A")
        df = pl.DataFrame({"col": [1.0, float("nan")]})
        plans = stringifier("col", "float")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1.0", "N/A"]

    def test_custom_esd_suffix(self) -> None:
        """Test custom esd_col_suffix option."""
        stringifier = Stringifier(esd_col_suffix="_unc")
        df = pl.DataFrame({
            "col": [1.234, 5.678],
            "col_unc": [5, None],
        })
        plans = stringifier("col", "float", has_esd=True)
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["1.234(5)", "5.678"]
        assert "col_unc" in [p.consumes for p in plans][0]

    def test_custom_date_format(self) -> None:
        """Test custom date_format option."""
        stringifier = Stringifier(date_format="%d/%m/%Y")
        df = pl.DataFrame({
            "col": pl.Series(["2023-05-20"]).str.strptime(pl.Date, "%Y-%m-%d")
        })
        plans = stringifier("col", "yyyy-mm-dd")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["20/05/2023"]

    def test_custom_datetime_format(self) -> None:
        """Test custom datetime_format option."""
        stringifier = Stringifier(datetime_format="%Y-%m-%d %H:%M")
        df = pl.DataFrame({
            "col": pl.Series(["2023-05-20 15:45"]).str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M"
            )
        })
        plans = stringifier("col", "yyyy-mm-dd:hh:mm")
        result = df.with_columns([p.expr for p in plans])
        assert result["col"].to_list() == ["2023-05-20 15:45"]
