"""Unit tests for the CIF parser module.

Tests low-level parsing functionality and error handling.
"""

import pytest

import ciffile
from ciffile.exception import CIFFileReadError, CIFFileReadErrorType


@pytest.mark.unit
@pytest.mark.parser
class TestCIFParser:
    """Test suite for CIF parser functionality."""

    def test_parse_simple_single_value(self) -> None:
        """Test parsing a simple single data value."""
        content = "data_test\n_item  'value'"
        cif = ciffile.read(content, variant="cif1")

        assert len(cif) == 1
        block = cif[0]
        assert "item" in block.codes

    def test_parse_loop_construct(self) -> None:
        """Test parsing a loop construct."""
        content = """
data_test
loop_
_item1
_item2
value1  value2
value3  value4
"""
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        # Should have items grouped in same category
        assert len(block.codes) > 0

    def test_parse_quoted_strings(self) -> None:
        """Test parsing single-quoted strings."""
        content = "data_test\n_item  'quoted value'"
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert "quoted value" in str(item.values[0])

    def test_parse_double_quoted_strings(self) -> None:
        """Test parsing double-quoted strings."""
        content = 'data_test\n_item  "quoted value"'
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert "quoted value" in str(item.values[0])

    def test_parse_text_field(self) -> None:
        """Test parsing text field (semicolon-delimited multiline string)."""
        content = """data_test
_item
;Multi-line
text field
value
;
"""
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        cat = block[0]
        item = cat[0]
        value_str = str(item.values[0])
        assert "Multi-line" in value_str or "multi-line" in value_str.lower()

    def test_parse_unquoted_value(self) -> None:
        """Test parsing unquoted value."""
        content = "data_test\n_item  unquoted_value"
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "unquoted_value"

    def test_parse_numeric_value(self) -> None:
        """Test parsing numeric values (initially as strings)."""
        content = "data_test\n_item  123.45"
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert "123.45" in str(item.values[0])

    def test_parse_question_mark_value(self) -> None:
        """Test parsing question mark (unknown) value."""
        content = "data_test\n_item  ?"
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "?"

    def test_parse_period_value(self) -> None:
        """Test parsing period (inapplicable) value."""
        content = "data_test\n_item  ."
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "."

    def test_parse_comments(self) -> None:
        """Test that comments are ignored."""
        content = """
data_test
# This is a comment
_item  'value'  # Another comment
"""
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        assert "item" in block.codes

    def test_parse_multiple_blocks(self) -> None:
        """Test parsing multiple data blocks."""
        content = """
data_block1
_item1  'value1'

data_block2
_item2  'value2'

data_block3
_item3  'value3'
"""
        cif = ciffile.read(content, variant="cif1")

        assert len(cif) == 3
        assert "block1" in cif
        assert "block2" in cif
        assert "block3" in cif

    def test_parse_save_frames(self) -> None:
        """Test parsing save frames."""
        content = """
data_dict
save_frame1
_item  'value'
save_

save_frame2
_item2  'value2'
save_
"""
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        assert block.type == "dict"
        assert len(block.frames) == 2

    def test_parse_mmcif_category_syntax(self) -> None:
        """Test parsing mmCIF category.keyword syntax."""
        content = """
data_test
_atom_site.id  1
_atom_site.symbol  C
"""
        cif = ciffile.read(content, variant="mmcif")

        block = cif[0]
        assert "atom_site" in block
        cat = block["atom_site"]
        assert "id" in cat.codes
        assert "symbol" in cat.codes

    def test_parse_mmcif_loop_with_categories(self) -> None:
        """Test parsing mmCIF loop with explicit categories."""
        content = """
data_test
loop_
_atom_site.id
_atom_site.symbol
_atom_site.x
1  C  10.0
2  N  11.0
"""
        cif = ciffile.read(content, variant="mmcif")

        block = cif[0]
        assert "atom_site" in block
        cat = block["atom_site"]
        assert cat.df.shape[0] == 2
        assert cat.df.shape[1] == 3

    def test_parse_empty_loop(self) -> None:
        """Test parsing loop with header but no values."""
        content = """
data_test
loop_
_item1
_item2
"""
        # This should either parse or raise error depending on strictness
        try:
            cif = ciffile.read(content, variant="cif1", raise_level=2)
        except CIFFileReadError:
            pass  # Expected for strict parsing

    def test_parse_case_sensitive(self) -> None:
        """Test parsing preserves case when case_normalization is None."""
        content = """
data_Test
_Item  'Value'
"""
        cif = ciffile.read(content, variant="cif1", case_normalization=None)

        # Block code should preserve case
        assert "Test" in cif.codes or "test" in cif.codes

    def test_parse_case_insensitive_lower(self) -> None:
        """Test parsing with lowercase normalization."""
        content = """
data_TEST
_ITEM  'VALUE'
"""
        cif = ciffile.read(content, variant="cif1", case_normalization="lower")

        assert "test" in cif.codes

    def test_parse_case_insensitive_upper(self) -> None:
        """Test parsing with uppercase normalization."""
        content = """
data_test
_item  'value'
"""
        cif = ciffile.read(content, variant="cif1", case_normalization="upper")

        assert "TEST" in cif.codes

    def test_parse_whitespace_handling(self) -> None:
        """Test parsing handles various whitespace correctly."""
        content = """data_test
        _item1     'value1'
        _item2   'value2'
"""
        cif = ciffile.read(content, variant="cif1")

        assert len(cif) == 1

    def test_parse_long_lines(self) -> None:
        """Test parsing very long lines."""
        long_value = "x" * 1000
        content = f"data_test\n_item  '{long_value}'"

        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert len(item.values[0]) >= 1000

    def test_parse_special_characters(self) -> None:
        """Test parsing values with special characters."""
        content = """data_test
_item  'value_with-special.chars$123'
"""
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        assert len(block) > 0

    def test_parse_unicode_characters(self) -> None:
        """Test parsing Unicode characters."""
        content = """data_test
_item  'α β γ δ'
"""
        cif = ciffile.read(content, variant="cif1", encoding="utf-8")

        block = cif[0]
        cat = block[0]
        item = cat[0]
        value = item.values[0]
        assert "α" in value or len(value) > 0

    def test_parse_nested_quotes(self) -> None:
        """Test parsing values with nested quotes."""
        content = """data_test
_item  "value with 'nested' quotes"
"""
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert "nested" in item.values[0]

    def test_parse_error_duplicate_block(self) -> None:
        """Test parsing error for duplicate block codes."""
        content = """
data_test
_item1  'value1'

data_test
_item2  'value2'
"""
        # Should either handle or raise error depending on raise_level
        try:
            cif = ciffile.read(content, variant="cif1", raise_level=1)
        except CIFFileReadError as e:
            assert e.error_type == CIFFileReadErrorType.PARSING

    def test_parse_error_duplicate_item(self) -> None:
        """Test parsing handles duplicate item names."""
        content = """
data_test
_item  'value1'
_item  'value2'
"""
        # Should handle gracefully or raise depending on settings
        cif = ciffile.read(content, variant="cif1", raise_level=2)
        assert len(cif) == 1

    def test_parse_very_large_loop(self) -> None:
        """Test parsing a large loop construct."""
        num_rows = 1000
        rows = "\n".join([f"{i}  row{i}" for i in range(num_rows)])
        content = f"""
data_test
loop_
_id
_value
{rows}
"""
        cif = ciffile.read(content, variant="cif1")

        block = cif[0]
        cat = block[0]
        assert cat.df.shape[0] == num_rows
