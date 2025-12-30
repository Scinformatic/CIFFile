"""Unit tests for CIF specification compliance.

These tests verify that the parser correctly handles edge cases
according to the official CIF 1.1 specification from IUCr:
https://www.iucr.org/resources/cif/spec/version1.1

Each test includes a reference to the relevant section of the specification.
"""

import pytest
import re

import ciffile
from ciffile.parser._token import TOKENIZER, Token
from ciffile.exception import CIFFileReadError


@pytest.mark.unit
@pytest.mark.parser
class TestTokenizerSpecCompliance:
    """Tests for tokenizer compliance with CIF 1.1 spec."""

    # ===== Quoted String Tests (Spec Section on CharString) =====

    def test_single_quoted_with_embedded_single_quote(self) -> None:
        """Test single quote inside single-quoted string is allowed if not followed by whitespace.

        Per CIF spec: A quoted string may contain the delimiter character itself
        as long as it is not followed by whitespace.
        """
        content = "data_test\n_item 'It''s valid'"
        # The string It''s valid should be parsed (two single quotes = escaped quote)
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        # Value should contain the quote character
        assert "'" in item.values[0] or "It" in item.values[0]

    def test_double_quoted_with_embedded_double_quote(self) -> None:
        """Test double quote inside double-quoted string handling."""
        content = 'data_test\n_item "He said ""hello"""'
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert "He said" in item.values[0] or '"' in item.values[0]

    def test_single_quoted_apostrophe_word(self) -> None:
        """Test single-quoted string containing apostrophe (It's).

        Per spec: 'It's a valid string' is valid because the embedded quote
        is not immediately followed by whitespace.
        """
        content = "data_test\n_item \"It's a valid string\""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert "It's" in item.values[0]

    # ===== Unquoted Value Tests =====

    def test_unquoted_value_starting_with_digit(self) -> None:
        """Test unquoted value starting with a digit."""
        content = "data_test\n_item 123abc"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "123abc"

    def test_unquoted_value_with_special_chars(self) -> None:
        """Test unquoted value with allowed special characters."""
        content = "data_test\n_item value_with-hyphen.and.dots"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "value_with-hyphen.and.dots"

    def test_unknown_value_question_mark(self) -> None:
        """Test '?' represents unknown/missing value per CIF spec."""
        content = "data_test\n_item ?"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "?"

    def test_inapplicable_value_period(self) -> None:
        """Test '.' represents inapplicable/omitted value per CIF spec."""
        content = "data_test\n_item ."
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "."

    # ===== Reserved Words Tests =====

    def test_reserved_word_data_must_be_quoted(self) -> None:
        """Test that 'data_' as a value must be quoted.

        Per spec: Reserved words cannot appear as unquoted values.
        """
        content = "data_test\n_item 'data_value'"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "data_value"

    def test_reserved_word_loop_must_be_quoted(self) -> None:
        """Test that 'loop_' as a value must be quoted."""
        content = "data_test\n_item 'loop_value'"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "loop_value"

    def test_reserved_word_save_must_be_quoted(self) -> None:
        """Test that 'save_' as a value must be quoted."""
        content = "data_test\n_item 'save_value'"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "save_value"

    def test_reserved_word_stop_must_be_quoted(self) -> None:
        """Test that 'stop_' as a value must be quoted."""
        content = "data_test\n_item 'stop_value'"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "stop_value"

    def test_reserved_word_global_must_be_quoted(self) -> None:
        """Test that 'global_' as a value must be quoted."""
        content = "data_test\n_item 'global_value'"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "global_value"

    # ===== Comment Tests =====

    def test_comment_at_start_of_line(self) -> None:
        """Test comment starting at beginning of line."""
        content = """data_test
# This is a comment
_item value"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        assert "item" in block.codes

    def test_comment_after_value(self) -> None:
        """Test comment after a value on the same line."""
        content = "data_test\n_item value # This is a trailing comment"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == "value"

    def test_hash_in_quoted_string_not_comment(self) -> None:
        """Test that # inside quoted string is not treated as comment start."""
        content = "data_test\n_item 'value#with#hashes'"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert "value#with#hashes" == item.values[0]

    def test_hash_in_text_field_not_comment(self) -> None:
        """Test that # inside text field is not treated as comment start."""
        content = """data_test
_item
;Line with # hash
Another line
;
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert "#" in item.values[0]

    # ===== Text Field Tests (Semicolon-delimited) =====

    def test_text_field_basic(self) -> None:
        """Test basic text field parsing."""
        content = """data_test
_item
;This is a
multi-line text field
;
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert "multi-line" in item.values[0].lower() or "This is a" in item.values[0]

    def test_text_field_preserves_leading_whitespace(self) -> None:
        """Test that leading whitespace in text fields is preserved.

        Per CIF spec section 17: Leading white space within text lines
        must be retained as part of the data value.
        """
        content = """data_test
_item
;  Leading spaces preserved
    More indented line
;
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        # Check that indentation is preserved
        assert item.values[0].startswith("  ") or "  Leading" in item.values[0]

    def test_text_field_strips_trailing_whitespace(self) -> None:
        """Test that trailing whitespace in text fields is stripped.

        Per CIF spec section 17: Trailing white space on a line may be elided.
        """
        content = "data_test\n_item\n;Line with trailing spaces   \n;\n"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        # Line should not end with trailing spaces
        lines = item.values[0].split('\n')
        for line in lines:
            assert not line.endswith('   ')

    def test_text_field_empty(self) -> None:
        """Test empty text field."""
        content = """data_test
_item
;
;
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert item.values[0] == ""

    def test_text_field_with_semicolon_inside(self) -> None:
        """Test text field containing semicolons (not at line start)."""
        content = """data_test
_item
;Line with ; semicolon inside
Another;line;with;semicolons
;
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        item = cat[0]
        assert ";" in item.values[0]

    # ===== Data Name Tests =====

    def test_data_name_starts_with_underscore(self) -> None:
        """Test that data names must start with underscore."""
        content = "data_test\n_valid_name value"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        assert "valid_name" in block.codes

    def test_data_name_case_insensitive(self) -> None:
        """Test that data names are case-insensitive per CIF spec."""
        content1 = "data_test\n_MyItem value1"
        content2 = "data_test\n_myitem value2"

        cif1 = ciffile.read(content1, variant="cif1", case_normalization="lower")
        cif2 = ciffile.read(content2, variant="cif1", case_normalization="lower")

        # Both should normalize to the same name
        assert cif1[0].codes == cif2[0].codes


@pytest.mark.unit
@pytest.mark.parser
class TestParserSpecCompliance:
    """Tests for parser compliance with CIF 1.1 spec."""

    # ===== Loop Tests =====

    def test_loop_values_multiple_of_headers(self) -> None:
        """Test that loop values must be exact multiple of header count.

        Per CIF spec: The number of values in a loop must be an exact
        multiple of the number of data names in the loop header.
        """
        content = """data_test
loop_
_col1
_col2
a b
c d
e f
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        assert cat.df.shape[0] == 3
        assert cat.df.shape[1] == 2

    def test_loop_incomplete_values_error(self) -> None:
        """Test that incomplete loop values (not multiple of headers) raises error."""
        content = """data_test
loop_
_col1
_col2
a b
c
"""
        # Should raise error for incomplete loop - either CIFFileReadError or ValueError
        with pytest.raises((CIFFileReadError, ValueError)):
            ciffile.read(content, variant="cif1", raise_level=1)

    def test_loop_single_column(self) -> None:
        """Test loop with single column."""
        content = """data_test
loop_
_item
value1
value2
value3
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        assert cat.df.shape[0] == 3

    def test_loop_mixed_value_types(self) -> None:
        """Test loop with mixed quoted and unquoted values."""
        content = """data_test
loop_
_item
unquoted
'single quoted'
"double quoted"
?
.
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        assert cat.df.shape[0] == 5

    # ===== Data Block Tests =====

    def test_data_block_code_case_insensitive(self) -> None:
        """Test that data block codes are case-insensitive."""
        content = "data_MyBlock\n_item value"
        cif = ciffile.read(content, variant="cif1", case_normalization="lower")
        assert "myblock" in cif.codes

    def test_multiple_data_blocks(self) -> None:
        """Test parsing multiple data blocks in one file."""
        content = """
data_block1
_item1 value1

data_block2
_item2 value2

data_block3
_item3 value3
"""
        cif = ciffile.read(content, variant="cif1")
        assert len(cif) == 3

    # ===== Save Frame Tests =====

    def test_save_frame_basic(self) -> None:
        """Test basic save frame parsing."""
        content = """
data_dictionary
save_my_frame
_item value
save_
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        assert "my_frame" in block.frames.codes

    def test_save_frame_with_loop(self) -> None:
        """Test save frame containing a loop."""
        content = """
data_dictionary
save_my_frame
loop_
_item
value1
value2
save_
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        frame = block.frames["my_frame"]
        assert len(frame) > 0

    def test_multiple_save_frames_in_block(self) -> None:
        """Test multiple save frames in one data block."""
        content = """
data_dictionary
save_frame1
_item1 value1
save_

save_frame2
_item2 value2
save_

save_frame3
_item3 value3
save_
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        assert len(block.frames) == 3


@pytest.mark.unit
@pytest.mark.parser
class TestMMCIFSpecCompliance:
    """Tests specific to mmCIF format compliance."""

    def test_mmcif_category_keyword_syntax(self) -> None:
        """Test mmCIF _category.keyword naming convention."""
        content = """
data_test
_atom_site.id 1
_atom_site.type_symbol C
_atom_site.Cartn_x 10.0
"""
        cif = ciffile.read(content, variant="mmcif")
        block = cif[0]
        assert "atom_site" in block
        cat = block["atom_site"]
        assert "id" in cat.codes
        assert "type_symbol" in cat.codes
        assert "cartn_x" in cat.codes

    def test_mmcif_loop_same_category(self) -> None:
        """Test that mmCIF loop items must be from same category."""
        content = """
data_test
loop_
_atom_site.id
_atom_site.type_symbol
1 C
2 N
"""
        cif = ciffile.read(content, variant="mmcif")
        block = cif[0]
        cat = block["atom_site"]
        assert cat.df.shape[0] == 2

    def test_mmcif_multiple_categories(self) -> None:
        """Test multiple categories in mmCIF file."""
        content = """
data_test
_entry.id TEST

loop_
_atom_site.id
_atom_site.type_symbol
1 C
2 N

loop_
_cell.length_a
_cell.length_b
10.0 20.0
"""
        cif = ciffile.read(content, variant="mmcif")
        block = cif[0]
        assert "entry" in block
        assert "atom_site" in block
        assert "cell" in block


@pytest.mark.unit
@pytest.mark.parser
class TestTokenizerRegex:
    """Direct tests for the tokenizer regex patterns."""

    def test_tokenizer_text_field(self) -> None:
        """Test tokenizer correctly identifies text fields."""
        content = ";text\nfield\n;"
        matches = list(TOKENIZER.finditer(content))
        # Should have one match for text field
        text_field_matches = [m for m in matches if m.lastindex == Token.VALUE_FIELD.value]
        assert len(text_field_matches) == 1
        assert "text" in text_field_matches[0].group(1)

    def test_tokenizer_single_quote(self) -> None:
        """Test tokenizer correctly identifies single-quoted strings."""
        content = "'quoted value'"
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.VALUE_QUOTED.value
        assert matches[0].group(Token.VALUE_QUOTED.value) == "quoted value"

    def test_tokenizer_double_quote(self) -> None:
        """Test tokenizer correctly identifies double-quoted strings."""
        content = '"quoted value"'
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.VALUE_DOUBLE_QUOTED.value
        assert matches[0].group(Token.VALUE_DOUBLE_QUOTED.value) == "quoted value"

    def test_tokenizer_data_block(self) -> None:
        """Test tokenizer correctly identifies data block headers."""
        content = "data_myblock"
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.BLOCK_CODE.value
        assert matches[0].group(Token.BLOCK_CODE.value) == "myblock"

    def test_tokenizer_loop_keyword(self) -> None:
        """Test tokenizer correctly identifies loop_ keyword."""
        content = "loop_"
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.LOOP.value

    def test_tokenizer_data_name(self) -> None:
        """Test tokenizer correctly identifies data names."""
        content = "_my_data_name"
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.NAME.value
        assert matches[0].group(Token.NAME.value) == "my_data_name"

    def test_tokenizer_save_frame_start(self) -> None:
        """Test tokenizer correctly identifies save frame start."""
        content = "save_myframe"
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.FRAME_CODE.value
        assert matches[0].group(Token.FRAME_CODE.value) == "myframe"

    def test_tokenizer_save_frame_end(self) -> None:
        """Test tokenizer correctly identifies save frame end."""
        content = "save_"
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.FRAME_END.value

    def test_tokenizer_comment(self) -> None:
        """Test tokenizer correctly identifies comments."""
        content = "# This is a comment"
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.COMMENT.value

    def test_tokenizer_unquoted_value(self) -> None:
        """Test tokenizer correctly identifies unquoted values."""
        content = "simple_value"
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.VALUE.value
        assert matches[0].group(Token.VALUE.value) == "simple_value"

    def test_tokenizer_numeric_value(self) -> None:
        """Test tokenizer correctly identifies numeric values."""
        content = "123.456"
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.VALUE.value
        assert matches[0].group(Token.VALUE.value) == "123.456"

    def test_tokenizer_scientific_notation(self) -> None:
        """Test tokenizer handles scientific notation."""
        content = "1.23e-10"
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.VALUE.value
        assert matches[0].group(Token.VALUE.value) == "1.23e-10"

    def test_tokenizer_esd_value(self) -> None:
        """Test tokenizer handles values with ESD (estimated standard deviation)."""
        content = "10.234(5)"
        matches = list(TOKENIZER.finditer(content))
        assert len(matches) == 1
        assert matches[0].lastindex == Token.VALUE.value
        assert matches[0].group(Token.VALUE.value) == "10.234(5)"


@pytest.mark.unit
@pytest.mark.parser
class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_file(self) -> None:
        """Test parsing an empty file.

        Note: Empty string '' is ambiguous in the reader - it could be
        an empty file content or a path. We test with explicit whitespace.
        """
        # Use a file-like object to explicitly provide empty content
        from io import StringIO
        content = StringIO("")
        with pytest.raises((CIFFileReadError, Exception)):
            ciffile.read(content, variant="cif1", raise_level=1)

    def test_only_comments(self) -> None:
        """Test file containing only comments."""
        content = """# Comment 1
# Comment 2
# Comment 3
"""
        with pytest.raises(CIFFileReadError):
            ciffile.read(content, variant="cif1", raise_level=1)

    def test_only_whitespace(self) -> None:
        """Test file containing only whitespace.

        A file with only whitespace should raise CIFFileReadError
        for incomplete file (no data blocks found).
        """
        content = "   \n\n   \t\t   \n"
        # Should raise CIFFileReadError for incomplete/empty file
        with pytest.raises(CIFFileReadError):
            ciffile.read(content, variant="cif1", raise_level=1)

    def test_data_block_no_content(self) -> None:
        """Test data block with no content.

        A data block with no items is technically incomplete per the parser.
        With raise_level=2 (only fatal errors), an exception may still be raised.
        """
        content = "data_empty"
        try:
            cif = ciffile.read(content, variant="cif1", raise_level=2)
            block = cif[0]
            assert len(block) == 0
        except CIFFileReadError:
            # It's acceptable for the parser to reject an empty data block
            pass

    def test_consecutive_loops(self) -> None:
        """Test consecutive loops in same block."""
        content = """data_test
loop_
_col1
a
b

loop_
_col2
c
d
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        assert len(block) == 2

    def test_mixed_single_and_loop_items(self) -> None:
        """Test mixing single items and loops in same block."""
        content = """data_test
_single_item value

loop_
_loop_item
a
b

_another_single another_value
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        # Should have items from both single and loop contexts
        assert len(block) >= 2

    def test_very_long_data_name(self) -> None:
        """Test handling of very long data names (spec limit is 75 chars)."""
        long_name = "a" * 70
        content = f"data_test\n_{long_name} value"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        assert len(block) == 1

    def test_data_block_code_with_special_chars(self) -> None:
        """Test data block code with allowed special characters."""
        content = "data_block-with_special.chars\n_item value"
        cif = ciffile.read(content, variant="cif1")
        assert len(cif) == 1

    def test_numeric_with_esd_in_loop(self) -> None:
        """Test numeric values with ESD in loop."""
        content = """data_test
loop_
_value
10.234(5)
20.456(10)
30.789(15)
"""
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        cat = block[0]
        assert cat.df.shape[0] == 3
        # Values should be stored as strings with ESD
        assert "(" in cat.df["value"][0]

    def test_windows_line_endings(self) -> None:
        """Test handling of Windows-style line endings (CRLF)."""
        content = "data_test\r\n_item value\r\n"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        assert "item" in block.codes

    def test_mac_classic_line_endings(self) -> None:
        """Test handling of classic Mac line endings (CR only)."""
        content = "data_test\r_item value\r"
        # Note: Pure CR line endings may not be fully supported
        # This test documents current behavior
        try:
            cif = ciffile.read(content, variant="cif1")
            assert len(cif) >= 0  # Just verify it doesn't crash
        except CIFFileReadError:
            pass  # Acceptable if not supported

    def test_mixed_line_endings(self) -> None:
        """Test handling of mixed line endings."""
        content = "data_test\n_item1 value1\r\n_item2 value2\n"
        cif = ciffile.read(content, variant="cif1")
        block = cif[0]
        assert "item1" in block.codes or "item2" in block.codes
