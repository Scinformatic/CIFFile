"""Unit tests for the CIF file creator module.

Tests the ciffile.create() function and CIF file construction functionality.
"""

from typing import Dict, List, Any
import pytest
import polars as pl
import pandas as pd

import ciffile
from ciffile import CIFFile


@pytest.mark.unit
@pytest.mark.creator
class TestCIFCreator:
    """Test suite for CIF file creation functionality."""

    def test_create_from_dict(self) -> None:
        """Test creating a CIF file from a dictionary."""
        data = {
            "block": ["test_data"] * 3,
            "category": ["cat1"] * 3,
            "keyword": ["key1", "key2", "key3"],
            "values": [["val1"], ["val2"], ["val3"]],
        }

        cif = ciffile.create(data, variant="mmcif")

        assert isinstance(cif, CIFFile)
        assert len(cif) == 1
        assert "test_data" in cif

    def test_create_from_polars_dataframe(self, sample_dataframe: pl.DataFrame) -> None:
        """Test creating a CIF file from a Polars DataFrame.

        Parameters
        ----------
        sample_dataframe : pl.DataFrame
            Sample DataFrame fixture.
        """
        cif = ciffile.create(sample_dataframe, variant="mmcif")

        assert isinstance(cif, CIFFile)
        assert len(cif) == 1
        block = cif[0]
        assert "cat1" in block
        assert "cat2" in block

    def test_create_from_pandas_dataframe(self) -> None:
        """Test creating a CIF file from a Pandas DataFrame."""
        data = pd.DataFrame({
            "block": ["test_data"] * 3,
            "category": ["cat1"] * 3,
            "keyword": ["key1", "key2", "key3"],
            "values": [["val1"], ["val2"], ["val3"]],
        })

        cif = ciffile.create(data, variant="mmcif")

        assert isinstance(cif, CIFFile)
        assert len(cif) == 1

    def test_create_from_list_of_dicts(self) -> None:
        """Test creating a CIF file from a list of dictionaries."""
        data = [
            {"block": "test", "category": "cat1", "keyword": "k1", "values": ["v1"]},
            {"block": "test", "category": "cat1", "keyword": "k2", "values": ["v2"]},
            {"block": "test", "category": "cat2", "keyword": "k3", "values": ["v3"]},
        ]

        cif = ciffile.create(data, variant="mmcif")

        assert isinstance(cif, CIFFile)
        assert len(cif) == 1

    def test_create_mmcif_variant(self) -> None:
        """Test creating a CIF file with mmCIF variant."""
        data = {
            "block": ["test_data"] * 2,
            "category": ["atom_site"] * 2,
            "keyword": ["id", "symbol"],
            "values": [["1", "2"], ["C", "N"]],
        }

        cif = ciffile.create(data, variant="mmcif")

        assert cif._variant == "mmcif"
        block = cif[0]
        assert "atom_site" in block

    def test_create_cif1_variant(self) -> None:
        """Test creating a CIF file with CIF 1.1 variant."""
        data = {
            "block": ["test_data"] * 2,
            "category": ["1"] * 2,  # Numeric category for CIF1 loop
            "keyword": ["item1", "item2"],
            "values": [["val1"], ["val2"]],
        }

        cif = ciffile.create(data, variant="cif1")

        assert cif._variant == "cif1"

    def test_create_with_validation(self) -> None:
        """Test creating a CIF file with validation enabled."""
        data = {
            "block": ["test_data"] * 2,
            "category": ["cat1"] * 2,
            "keyword": ["key1", "key2"],
            "values": [["val1"], ["val2"]],
        }

        cif = ciffile.create(data, variant="mmcif", validate=True)

        assert isinstance(cif, CIFFile)

    def test_create_without_validation(self) -> None:
        """Test creating a CIF file with validation disabled."""
        data = {
            "block": ["test_data"] * 2,
            "category": ["cat1"] * 2,
            "keyword": ["key1", "key2"],
            "values": [["val1"], ["val2"]],
        }

        cif = ciffile.create(data, variant="mmcif", validate=False)

        assert isinstance(cif, CIFFile)

    def test_create_with_custom_column_names(self) -> None:
        """Test creating a CIF file with custom column names."""
        data = {
            "my_block": ["test_data"] * 2,
            "my_cat": ["cat1"] * 2,
            "my_key": ["key1", "key2"],
            "my_vals": [["val1"], ["val2"]],
        }

        cif = ciffile.create(
            data,
            variant="mmcif",
            col_name_block="my_block",
            col_name_cat="my_cat",
            col_name_key="my_key",
            col_name_values="my_vals",
        )

        assert isinstance(cif, CIFFile)
        assert len(cif) == 1

    def test_create_with_multiple_blocks(self) -> None:
        """Test creating a CIF file with multiple data blocks."""
        data = {
            "block": ["block1", "block1", "block2", "block2"],
            "category": ["cat1"] * 4,
            "keyword": ["key1", "key2", "key1", "key2"],
            "values": [["val1"], ["val2"], ["val3"], ["val4"]],
        }

        cif = ciffile.create(data, variant="mmcif")

        assert len(cif) == 2
        assert "block1" in cif
        assert "block2" in cif

    def test_create_with_save_frames(self) -> None:
        """Test creating a CIF dictionary file with save frames."""
        data = {
            "block": ["dict_block"] * 4,
            "frame": ["frame1", "frame1", "frame2", "frame2"],
            "category": ["cat1"] * 4,
            "keyword": ["key1", "key2", "key1", "key2"],
            "values": [["val1"], ["val2"], ["val3"], ["val4"]],
        }

        cif = ciffile.create(data, variant="cif1")

        assert len(cif) == 1
        block = cif[0]
        assert block.type == "dict"
        assert len(block.frames) == 2

    def test_create_with_loop_data(self) -> None:
        """Test creating a CIF file with tabular (loop) data."""
        data = {
            "block": ["test_data"] * 3,
            "category": ["atom_site"] * 3,
            "keyword": ["id", "symbol", "x"],
            "values": [["1", "2", "3"], ["C", "N", "O"], ["10.0", "11.0", "12.0"]],
        }

        cif = ciffile.create(data, variant="mmcif")

        block = cif[0]
        atom_site = block["atom_site"]

        # Should create a table with 3 rows
        assert atom_site.df.shape[0] == 3
        assert atom_site.df.shape[1] == 3

    def test_create_with_single_values(self) -> None:
        """Test creating a CIF file with single (non-loop) values."""
        data = {
            "block": ["test_data"] * 3,
            "category": ["entry", "entry", "entry"],
            "keyword": ["id", "title", "author"],
            "values": [["TEST"], ["Test Structure"], ["John Doe"]],
        }

        cif = ciffile.create(data, variant="mmcif")

        block = cif[0]
        entry = block["entry"]

        # Should create single values
        assert entry.df.shape[0] == 1
        assert entry.df.shape[1] == 3

    def test_create_preserves_data_types(self) -> None:
        """Test that create preserves data as strings initially."""
        data = {
            "block": ["test_data"] * 3,
            "category": ["data"] * 3,
            "keyword": ["int_val", "float_val", "str_val"],
            "values": [["10"], ["10.5"], ["text"]],
        }

        cif = ciffile.create(data, variant="mmcif")

        block = cif[0]
        data_cat = block["data"]
        df = data_cat.df

        # All values should initially be strings
        for col in df.columns:
            assert df[col].dtype == pl.String or df[col].dtype == pl.List(pl.String)

    def test_create_empty_values_list(self) -> None:
        """Test creating a CIF file with empty values lists."""
        data = {
            "block": ["test_data"],
            "category": ["cat1"],
            "keyword": ["key1"],
            "values": [[]],
        }

        cif = ciffile.create(data, variant="mmcif", validate=False)

        # Should handle empty values appropriately
        assert isinstance(cif, CIFFile)

    def test_create_roundtrip(self, sample_mmcif_content: str) -> None:
        """Test that create can reconstruct a file from read output.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample CIF content fixture.
        """
        # Read original file
        cif_original = ciffile.read(sample_mmcif_content)

        # Get DataFrame representation
        df = cif_original.df

        # Create new CIF from DataFrame
        cif_recreated = ciffile.create(
            df,
            variant="mmcif",
            col_name_block=cif_original._col_block,
            col_name_cat=cif_original._col_cat,
            col_name_key=cif_original._col_key,
            col_name_values=cif_original._col_values,
        )

        # Compare structures
        assert len(cif_original) == len(cif_recreated)
        assert cif_original.codes == cif_recreated.codes
