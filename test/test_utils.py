"""Unit tests for utility functions and helper modules."""

import pytest
import polars as pl

from ciffile._helper import normalize_whitespace


@pytest.mark.unit
class TestHelperFunctions:
    """Test suite for helper utility functions."""

    def test_normalize_whitespace_string(self) -> None:
        """Test normalizing whitespace in a single string."""
        input_str = "  test  \n  string  "
        result = normalize_whitespace(input_str, df=None)

        assert result == "test string"

    def test_normalize_whitespace_with_newlines(self) -> None:
        """Test normalizing whitespace with newlines."""
        input_str = "line1\nline2\nline3"
        result = normalize_whitespace(input_str, df=None)

        assert result == "line1 line2 line3"

    def test_normalize_whitespace_with_tabs(self) -> None:
        """Test normalizing whitespace with tabs."""
        input_str = "word1\t\tword2"
        result = normalize_whitespace(input_str, df=None)

        assert result == "word1 word2"

    def test_normalize_whitespace_multiple_spaces(self) -> None:
        """Test normalizing multiple consecutive spaces."""
        input_str = "word1     word2"
        result = normalize_whitespace(input_str, df=None)

        assert result == "word1 word2"

    def test_normalize_whitespace_expression(self) -> None:
        """Test getting normalization expression."""
        import polars as pl
        expr = normalize_whitespace(pl.col("col_name"), df=None)

        assert isinstance(expr, pl.Expr)

    def test_normalize_whitespace_dataframe_single_column(self) -> None:
        """Test normalizing whitespace in a single DataFrame column."""
        df = pl.DataFrame({
            "text": ["  hello  \n  world  ", "foo\tbar"]
        })

        result = normalize_whitespace("text", df=df)

        assert result["text"][0] == "hello world"
        assert result["text"][1] == "foo bar"

    def test_normalize_whitespace_dataframe_multiple_columns(self) -> None:
        """Test normalizing whitespace in multiple DataFrame columns."""
        df = pl.DataFrame({
            "col1": ["  text1  "],
            "col2": ["  text2  \n  continued  "]
        })

        result = normalize_whitespace(["col1", "col2"], df=df)

        assert result["col1"][0] == "text1"
        assert result["col2"][0] == "text2 continued"

    def test_normalize_whitespace_empty_string(self) -> None:
        """Test normalizing an empty string."""
        result = normalize_whitespace("", df=None)

        assert result == ""

    def test_normalize_whitespace_only_whitespace(self) -> None:
        """Test normalizing a string with only whitespace."""
        input_str = "   \n\t\r  "
        result = normalize_whitespace(input_str, df=None)

        assert result == ""

    def test_normalize_whitespace_preserves_single_spaces(self) -> None:
        """Test that single spaces are preserved."""
        input_str = "word1 word2 word3"
        result = normalize_whitespace(input_str, df=None)

        assert result == "word1 word2 word3"

    def test_normalize_whitespace_mixed_line_endings(self) -> None:
        """Test normalizing mixed line ending styles."""
        input_str = "line1\rline2\r\nline3\nline4"
        result = normalize_whitespace(input_str, df=None)

        assert result == "line1 line2 line3 line4"


@pytest.mark.unit
class TestDataFrameConversion:
    """Test suite for DataFrame conversion utilities."""

    def test_to_id_dict_single_id(self, sample_category) -> None:
        """Test converting DataFrame to dictionary with single ID.

        Parameters
        ----------
        sample_category
            Sample category fixture.
        """
        if len(sample_category.codes) > 0 and sample_category.df.shape[0] > 0:
            first_key = sample_category.codes[0]
            result = sample_category.to_id_dict(ids=first_key)

            assert isinstance(result, dict)

    def test_to_id_dict_multiple_ids_flat(self, sample_category) -> None:
        """Test converting DataFrame to flat dictionary with multiple IDs.

        Parameters
        ----------
        sample_category
            Sample category fixture.
        """
        if len(sample_category.codes) >= 2 and sample_category.df.shape[0] > 0:
            keys = sample_category.codes[:2]
            result = sample_category.to_id_dict(ids=keys, flat=True)

            assert isinstance(result, dict)

    def test_to_id_dict_multiple_ids_nested(self, sample_category) -> None:
        """Test converting DataFrame to nested dictionary with multiple IDs.

        Parameters
        ----------
        sample_category
            Sample category fixture.
        """
        if len(sample_category.codes) >= 2 and sample_category.df.shape[0] > 0:
            keys = sample_category.codes[:2]
            result = sample_category.to_id_dict(ids=keys, flat=False)

            assert isinstance(result, dict)


@pytest.mark.unit
class TestCategoryExtraction:
    """Test suite for category extraction utilities."""

    def test_category_extraction_single(self, sample_cif_file) -> None:
        """Test extracting a single category.

        Parameters
        ----------
        sample_cif_file
            Sample CIF file fixture.
        """
        block = sample_cif_file[0]
        if len(block.codes) > 0:
            cat_name = block.codes[0]
            result = sample_cif_file.category(cat_name)

            from ciffile import CIFDataCategory
            assert isinstance(result, CIFDataCategory)

    def test_category_extraction_multiple(self, sample_cif_file) -> None:
        """Test extracting multiple categories.

        Parameters
        ----------
        sample_cif_file
            Sample CIF file fixture.
        """
        block = sample_cif_file[0]
        if len(block.codes) >= 2:
            cat_names = block.codes[:2]
            result = sample_cif_file.category(*cat_names)

            assert isinstance(result, dict)
            assert len(result) == 2

    def test_category_extraction_with_block_column(self, sample_cif_file) -> None:
        """Test category extraction includes block column.

        Parameters
        ----------
        sample_cif_file
            Sample CIF file fixture.
        """
        block = sample_cif_file[0]
        if len(block.codes) > 0:
            cat_name = block.codes[0]
            result = sample_cif_file.category(cat_name, col_name_block="_block")

            assert "_block" in result.df.columns or result.df.shape[0] == 0

    def test_category_extraction_drop_redundant(self, sample_cif_file) -> None:
        """Test category extraction with drop_redundant option.

        Parameters
        ----------
        sample_cif_file
            Sample CIF file fixture.
        """
        block = sample_cif_file[0]
        if len(block.codes) > 0:
            cat_name = block.codes[0]
            result = sample_cif_file.category(
                cat_name,
                col_name_block="_block",
                drop_redundant=True
            )

            # If there's only one block, block column should be dropped
            if len(sample_cif_file) == 1:
                assert "_block" not in result.df.columns or result.df.shape[0] == 0


@pytest.mark.unit
class TestFilePartIsolation:
    """Test suite for file part isolation functionality."""

    def test_part_data(self, sample_cif_file) -> None:
        """Test isolating data part of file.

        Parameters
        ----------
        sample_cif_file
            Sample CIF file fixture.
        """
        result = sample_cif_file.part("data")

        # Should return CIFFile or None
        assert result is None or isinstance(result, type(sample_cif_file))

    def test_part_dict(self, sample_cif_file) -> None:
        """Test isolating dictionary part of file.

        Parameters
        ----------
        sample_cif_file
            Sample CIF file fixture.
        """
        result = sample_cif_file.part("dict")

        # Should return CIFFile or None
        assert result is None or isinstance(result, type(sample_cif_file))

    def test_part_multiple(self, sample_cif_file) -> None:
        """Test isolating multiple parts of file.

        Parameters
        ----------
        sample_cif_file
            Sample CIF file fixture.
        """
        result = sample_cif_file.part("data", "dict")

        assert isinstance(result, dict)
        assert "data" in result
        assert "dict" in result

    def test_part_all(self, sample_cif_file) -> None:
        """Test isolating all parts of file.

        Parameters
        ----------
        sample_cif_file
            Sample CIF file fixture.
        """
        result = sample_cif_file.part()

        assert isinstance(result, dict)
        assert "data" in result
        assert "dict" in result
        assert "dict_cat" in result
        assert "dict_key" in result
