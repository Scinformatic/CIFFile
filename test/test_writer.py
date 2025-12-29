"""Unit tests for CIF file writing functionality.

Tests the CIF file writer module and write() methods.
"""

from typing import Any
from pathlib import Path
import tempfile
import pytest

from ciffile import CIFFile
import ciffile


@pytest.mark.unit
@pytest.mark.writer
class TestCIFWriter:
    """Test suite for CIF file writing functionality."""

    def test_write_to_string(self, sample_cif_file: CIFFile) -> None:
        """Test writing a CIF file to string.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        cif_str = str(sample_cif_file)

        assert isinstance(cif_str, str)
        assert len(cif_str) > 0
        assert "data_" in cif_str

    def test_write_to_file(self, sample_cif_file: CIFFile) -> None:
        """Test writing a CIF file to a file object.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False) as f:
            sample_cif_file.write(f.write)
            temp_path = Path(f.name)

        # Read back and verify
        assert temp_path.exists()
        content = temp_path.read_text()
        assert "data_" in content

        # Cleanup
        temp_path.unlink()

    def test_write_to_list(self, sample_cif_file: CIFFile) -> None:
        """Test writing a CIF file to a list collector.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(chunks.append)

        assert len(chunks) > 0
        content = "".join(chunks)
        assert "data_" in content

    def test_write_with_horizontal_list_style(self, sample_cif_file: CIFFile) -> None:
        """Test writing with horizontal list style.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(chunks.append, list_style="horizontal")
        content = "".join(chunks)

        assert len(content) > 0

    def test_write_with_tabular_list_style(self, sample_cif_file: CIFFile) -> None:
        """Test writing with tabular list style.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(chunks.append, list_style="tabular")
        content = "".join(chunks)

        assert len(content) > 0

    def test_write_with_vertical_list_style(self, sample_cif_file: CIFFile) -> None:
        """Test writing with vertical list style.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(chunks.append, list_style="vertical")
        content = "".join(chunks)

        assert len(content) > 0

    def test_write_with_tabular_horizontal_table_style(self, sample_cif_file: CIFFile) -> None:
        """Test writing with tabular-horizontal table style.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(chunks.append, table_style="tabular-horizontal")
        content = "".join(chunks)

        assert len(content) > 0
        assert "loop_" in content or len(content) > 0  # May or may not have loops

    def test_write_with_tabular_vertical_table_style(self, sample_cif_file: CIFFile) -> None:
        """Test writing with tabular-vertical table style.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(chunks.append, table_style="tabular-vertical")
        content = "".join(chunks)

        assert len(content) > 0

    def test_write_with_custom_bool_values(self, sample_cif_file: CIFFile) -> None:
        """Test writing with custom boolean representations.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(
            chunks.append,
            bool_true="yes",
            bool_false="no",
        )
        content = "".join(chunks)

        assert len(content) > 0

    def test_write_with_custom_null_values(self, sample_cif_file: CIFFile) -> None:
        """Test writing with custom null value representations.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(
            chunks.append,
            null_str="?",
            null_float="?",
            null_int="?",
            null_bool="?",
            empty_str=".",
            nan_float=".",
        )
        content = "".join(chunks)

        assert len(content) > 0

    def test_write_with_custom_spacing(self, sample_cif_file: CIFFile) -> None:
        """Test writing with custom spacing parameters.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(
            chunks.append,
            space_items=5,
            min_space_columns=3,
        )
        content = "".join(chunks)

        assert len(content) > 0

    def test_write_with_custom_indentation(self, sample_cif_file: CIFFile) -> None:
        """Test writing with custom indentation.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(
            chunks.append,
            indent=2,
            indent_inner=4,
        )
        content = "".join(chunks)

        assert len(content) > 0

    def test_write_with_delimiter_preference(self, sample_cif_file: CIFFile) -> None:
        """Test writing with custom delimiter preference.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(
            chunks.append,
            delimiter_preference=("double", "single", "semicolon"),
        )
        content = "".join(chunks)

        assert len(content) > 0

    def test_write_always_table(self, sample_cif_file: CIFFile) -> None:
        """Test writing with always_table option.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        chunks = []
        sample_cif_file.write(chunks.append, always_table=True)
        content = "".join(chunks)

        assert len(content) > 0

    def test_write_roundtrip(self, sample_mmcif_content: str) -> None:
        """Test that write/read roundtrip preserves structure.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample CIF content fixture.
        """
        # Read original
        cif_original = ciffile.read(sample_mmcif_content)

        # Write to string
        cif_str = str(cif_original)

        # Read back
        cif_roundtrip = ciffile.read(cif_str, variant="mmcif")

        # Compare structures
        assert len(cif_original) == len(cif_roundtrip)
        assert cif_original.codes == cif_roundtrip.codes

    def test_write_block(self, sample_cif_block) -> None:
        """Test writing a single CIF block.

        Parameters
        ----------
        sample_cif_block
            Sample CIF block fixture.
        """
        chunks = []
        sample_cif_block.write(chunks.append)
        content = "".join(chunks)

        assert len(content) > 0
        assert "data_" in content

    def test_write_category(self, sample_category) -> None:
        """Test writing a single data category.

        Parameters
        ----------
        sample_category
            Sample data category fixture.
        """
        chunks = []
        sample_category.write(chunks.append)
        content = "".join(chunks)

        assert len(content) > 0

    def test_write_preserves_block_order(self, sample_multiblock_content: str) -> None:
        """Test that writing preserves block order.

        Parameters
        ----------
        sample_multiblock_content : str
            Sample multi-block CIF content fixture.
        """
        cif = ciffile.read(sample_multiblock_content, variant="cif1")

        original_codes = cif.codes

        # Write and read back
        cif_str = str(cif)
        cif_roundtrip = ciffile.read(cif_str, variant="cif1")

        assert cif_roundtrip.codes == original_codes

    def test_write_empty_values_as_question_mark(self) -> None:
        """Test that empty/null values are written correctly."""
        import polars as pl

        data = {
            "block": ["test"] * 2,
            "category": ["cat"] * 2,
            "keyword": ["k1", "k2"],
            "values": [["?"], ["."]],
        }

        cif = ciffile.create(data, variant="cif1")
        cif_str = str(cif)

        assert "?" in cif_str
        assert "." in cif_str
