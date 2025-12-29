"""Unit tests for the CIF file reader module.

Tests the ciffile.read() function and related parsing functionality.
"""

from typing import Any
from pathlib import Path
import pytest
import polars as pl

import ciffile
from ciffile import CIFFile, CIFBlock
from ciffile.exception import CIFFileReadError


@pytest.mark.unit
@pytest.mark.parser
class TestCIFReader:
    """Test suite for CIF file reading functionality."""

    def test_read_from_string(self, sample_mmcif_content: str) -> None:
        """Test reading a CIF file from a string.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample CIF content fixture.
        """
        cif = ciffile.read(sample_mmcif_content)

        assert isinstance(cif, CIFFile)
        assert len(cif) == 1
        assert "test_structure" in cif

    def test_read_from_path(self, temp_cif_file: Path) -> None:
        """Test reading a CIF file from a file path.

        Parameters
        ----------
        temp_cif_file : Path
            Path to temporary CIF file fixture.
        """
        cif = ciffile.read(temp_cif_file)

        assert isinstance(cif, CIFFile)
        assert len(cif) == 1

    def test_read_from_file_object(self, temp_cif_file: Path) -> None:
        """Test reading a CIF file from a file object.

        Parameters
        ----------
        temp_cif_file : Path
            Path to temporary CIF file fixture.
        """
        with open(temp_cif_file, 'r') as f:
            cif = ciffile.read(f)

        assert isinstance(cif, CIFFile)
        assert len(cif) == 1

    def test_read_cif1_variant(self, sample_cif1_content: str) -> None:
        """Test reading a CIF 1.1 format file.

        Parameters
        ----------
        sample_cif1_content : str
            Sample CIF 1.1 content fixture.
        """
        cif = ciffile.read(sample_cif1_content, variant="cif1")

        assert cif._variant == "cif1"
        assert len(cif) == 1
        block = cif[0]

        # Check single items (category = item name for CIF1 single items)
        assert "single_item_1" in block.codes

        # Check loop items (category = loop ID for CIF1 loops)
        # Loop items are grouped under numeric category IDs
        assert "1" in block.codes

    def test_read_mmcif_variant(self, sample_mmcif_content: str) -> None:
        """Test reading an mmCIF format file.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample mmCIF content fixture.
        """
        cif = ciffile.read(sample_mmcif_content, variant="mmcif")

        assert cif._variant == "mmcif"
        assert len(cif) == 1
        block = cif[0]

        # Check categories
        assert "entry" in block
        assert "atom_site" in block
        assert "cell" in block

    def test_read_multiblock_file(self, sample_multiblock_content: str) -> None:
        """Test reading a CIF file with multiple data blocks.

        Parameters
        ----------
        sample_multiblock_content : str
            Sample multi-block CIF content fixture.
        """
        cif = ciffile.read(sample_multiblock_content, variant="cif1")

        assert len(cif) == 3
        assert "block_1" in cif
        assert "block_2" in cif
        assert "block_3" in cif

    def test_read_with_case_normalization_lower(self, sample_mmcif_content: str) -> None:
        """Test reading with lowercase normalization.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample CIF content fixture.
        """
        cif = ciffile.read(sample_mmcif_content, case_normalization="lower")
        block = cif[0]

        # Category names should be lowercase
        for code in block.codes:
            assert code.islower() or not code.isalpha()

    def test_read_with_case_normalization_upper(self, sample_mmcif_content: str) -> None:
        """Test reading with uppercase normalization.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample CIF content fixture.
        """
        cif = ciffile.read(sample_mmcif_content, case_normalization="upper")
        block = cif[0]

        # Category names should be uppercase
        for code in block.codes:
            assert code.isupper() or not code.isalpha()

    def test_read_with_case_normalization_none(self, sample_mmcif_content: str) -> None:
        """Test reading without case normalization.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample CIF content fixture.
        """
        cif = ciffile.read(sample_mmcif_content, case_normalization=None)

        # Should preserve original case
        assert isinstance(cif, CIFFile)

    def test_read_with_custom_column_names(self, sample_mmcif_content: str) -> None:
        """Test reading with custom column names.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample CIF content fixture.
        """
        cif = ciffile.read(
            sample_mmcif_content,
            col_name_block="my_block",
            col_name_cat="my_category",
            col_name_key="my_keyword",
            col_name_values="my_values",
        )

        df = cif.df
        assert "my_block" in df.columns
        assert "my_category" in df.columns
        assert "my_keyword" in df.columns
        assert "my_values" in df.columns

    def test_read_dict_file_with_frames(self, sample_dict_file_content: str) -> None:
        """Test reading a CIF dictionary file with save frames.

        Parameters
        ----------
        sample_dict_file_content : str
            Sample dictionary file content fixture.
        """
        cif = ciffile.read(sample_dict_file_content, variant="cif1")

        assert len(cif) == 1
        block = cif[0]
        assert block.type == "dict"
        assert len(block.frames) > 0

    def test_read_empty_string(self) -> None:
        """Test reading an empty string raises appropriate error."""
        with pytest.raises(Exception):  # Should raise some parsing error
            ciffile.read("")

    def test_read_invalid_cif_syntax(self) -> None:
        """Test reading invalid CIF syntax."""
        invalid_content = "data_test\n_item_without_value"

        # Should either raise error or handle gracefully depending on raise_level
        try:
            cif = ciffile.read(invalid_content, raise_level=2)
        except CIFFileReadError:
            pass  # Expected for raise_level=2

    def test_read_with_different_encodings(self, temp_cif_file: Path) -> None:
        """Test reading CIF files with different character encodings.

        Parameters
        ----------
        temp_cif_file : Path
            Path to temporary CIF file fixture.
        """
        # Test UTF-8 (default)
        cif_utf8 = ciffile.read(temp_cif_file, encoding="utf-8")
        assert isinstance(cif_utf8, CIFFile)

        # Test ASCII
        cif_ascii = ciffile.read(temp_cif_file, encoding="ascii")
        assert isinstance(cif_ascii, CIFFile)

    def test_read_preserves_data_structure(self, sample_mmcif_content: str) -> None:
        """Test that reading preserves the data structure correctly.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample CIF content fixture.
        """
        cif = ciffile.read(sample_mmcif_content)
        block = cif[0]

        # Check that atom_site category has correct structure
        atom_site = block["atom_site"]
        assert len(atom_site) == 5  # 5 columns
        assert "id" in atom_site.codes
        assert "type_symbol" in atom_site.codes

        # Check DataFrame shape
        df = atom_site.df
        assert df.shape[0] == 3  # 3 atoms
        assert df.shape[1] == 5  # 5 columns

    def test_read_dataframe_content(self, sample_mmcif_content: str) -> None:
        """Test that DataFrame content is correctly parsed.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample CIF content fixture.
        """
        cif = ciffile.read(sample_mmcif_content)
        block = cif[0]
        atom_site = block["atom_site"]

        df = atom_site.df

        # Check first row
        first_row = df.row(0, named=True)
        assert first_row["id"] == "1"
        assert first_row["type_symbol"] == "C"
        assert first_row["cartn_x"] == "10.0"
