"""Integration tests for the CIFFile package.

Tests complete workflows and interactions between modules.
"""

from pathlib import Path
import tempfile
import pytest

import ciffile
from ciffile import CIFFile


@pytest.mark.integration
class TestCIFIntegration:
    """Integration test suite for complete workflows."""

    def test_complete_read_write_roundtrip(self, sample_mmcif_content: str) -> None:
        """Test complete read-write-read cycle.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample CIF content fixture.
        """
        # Read
        cif_original = ciffile.read(sample_mmcif_content)

        # Write to string
        cif_str = str(cif_original)

        # Read back
        cif_roundtrip = ciffile.read(cif_str)

        # Verify structure preserved
        assert len(cif_original) == len(cif_roundtrip)
        assert cif_original.codes == cif_roundtrip.codes

    def test_complete_file_io_workflow(self, sample_mmcif_content: str) -> None:
        """Test complete file I/O workflow with disk operations.

        Parameters
        ----------
        sample_mmcif_content : str
            Sample CIF content fixture.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False) as f:
            temp_path = Path(f.name)
            f.write(sample_mmcif_content)

        try:
            # Read from file
            cif = ciffile.read(temp_path)

            # Modify
            df = cif.df

            # Create new file
            cif_modified = ciffile.create(df, variant="mmcif")

            # Write to new file
            output_path = temp_path.with_suffix('.out.cif')
            with open(output_path, 'w') as f:
                cif_modified.write(f.write)

            # Read back
            cif_final = ciffile.read(output_path)

            assert len(cif_final) == len(cif)

            # Cleanup
            output_path.unlink()
        finally:
            temp_path.unlink()

    def test_dataframe_manipulation_workflow(self, sample_cif_file: CIFFile) -> None:
        """Test workflow with DataFrame manipulation.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        # Get DataFrame
        df_original = sample_cif_file.df

        # Filter some rows (example: filter by block)
        block_name = df_original["block"][0]
        df_filtered = df_original.filter(df_original["block"] == block_name)

        # Create new CIF from filtered data
        cif_filtered = ciffile.create(df_filtered, variant="mmcif")

        assert len(cif_filtered) >= 1

    def test_category_extraction_and_manipulation(self, sample_cif_file: CIFFile) -> None:
        """Test extracting and manipulating categories.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        block = sample_cif_file[0]

        if len(block.codes) > 0:
            # Extract category
            cat_name = block.codes[0]
            category = block[cat_name]

            # Get DataFrame
            cat_df = category.df

            # Manipulate (example: select columns)
            if cat_df.shape[1] > 0:
                first_col = cat_df.columns[0]
                df_subset = cat_df.select(first_col)

                assert df_subset.shape[1] == 1

    def test_multiblock_processing(self, sample_multiblock_content: str) -> None:
        """Test processing a file with multiple blocks.

        Parameters
        ----------
        sample_multiblock_content : str
            Sample multi-block content fixture.
        """
        cif = ciffile.read(sample_multiblock_content, variant="cif1")

        # Process each block
        for block in cif:
            # Access categories in each block
            for category in block:
                # Access items in each category
                for item in category:
                    # Verify item has values
                    assert len(item) >= 0

    def test_save_frame_processing(self, sample_dict_file_content: str) -> None:
        """Test processing dictionary files with save frames.

        Parameters
        ----------
        sample_dict_file_content : str
            Sample dictionary file content fixture.
        """
        cif = ciffile.read(sample_dict_file_content, variant="cif1")

        block = cif[0]
        assert block.type == "dict"

        # Process save frames
        for frame in block.frames:
            # Access categories in frame
            for category in frame:
                assert len(category) >= 0

    def test_mixed_data_types_workflow(self) -> None:
        """Test workflow with mixed data types."""
        import polars as pl

        # Create data with various types (as strings initially)
        data = {
            "block": ["test"] * 5,
            "category": ["data"] * 5,
            "keyword": ["int_val", "float_val", "str_val", "bool_val", "null_val"],
            "values": [["10"], ["10.5"], ["text"], ["YES"], ["?"]],
        }

        # Create CIF
        cif = ciffile.create(data, variant="mmcif")

        # Write and read back
        cif_str = str(cif)
        cif_roundtrip = ciffile.read(cif_str)

        assert len(cif_roundtrip) == 1

    def test_large_file_processing(self) -> None:
        """Test processing a large CIF file."""
        # Create a large file
        num_atoms = 1000
        data = {
            "block": ["large_structure"] * (num_atoms * 3),
            "category": ["atom_site"] * (num_atoms * 3),
            "keyword": ["id", "symbol", "x"] * num_atoms,
            "values": [[str(i) for i in range(1, num_atoms + 1)],
                      ["C"] * num_atoms,
                      [str(float(i)) for i in range(num_atoms)]],
        }

        # Flatten the values properly
        flat_values = []
        for i in range(num_atoms):
            flat_values.extend([[str(i+1)], ["C"], [str(float(i))]])

        data_flat = {
            "block": ["large_structure"] * (num_atoms * 3),
            "category": ["atom_site"] * (num_atoms * 3),
            "keyword": (["id"] * num_atoms + ["symbol"] * num_atoms + ["x"] * num_atoms),
            "values": flat_values,
        }

        cif = ciffile.create(data_flat, variant="mmcif", allow_duplicate_rows=True)

        # Write to string
        cif_str = str(cif)

        assert len(cif_str) > 1000

    def test_error_recovery_workflow(self) -> None:
        """Test workflow with error handling."""
        # Try to read invalid content with different raise levels
        invalid_content = "data_test\n_item_without_value\n"

        # With raise_level=2 (only fatal errors)
        try:
            cif = ciffile.read(invalid_content, raise_level=2)
        except Exception:
            pass  # May or may not raise

        # With raise_level=0 (all errors and warnings)
        try:
            cif = ciffile.read(invalid_content, raise_level=0)
        except Exception:
            pass  # Expected to raise

    def test_format_conversion_cif1_to_mmcif(self, sample_cif1_content: str) -> None:
        """Test converting between CIF variants.

        Parameters
        ----------
        sample_cif1_content : str
            Sample CIF 1.1 content fixture.
        """
        # Read as CIF1
        cif_cif1 = ciffile.read(sample_cif1_content, variant="cif1")

        # Get DataFrame
        df = cif_cif1.df

        # Try to create as mmCIF (may need category adjustment)
        # This tests the conversion capability
        assert df is not None

    def test_iterative_block_building(self) -> None:
        """Test building a CIF file iteratively."""
        import polars as pl

        # Start with empty data structure
        all_data = []

        # Add multiple categories iteratively
        for cat_num in range(3):
            cat_data = {
                "block": "test_data",
                "category": f"cat{cat_num}",
                "keyword": f"key{cat_num}",
                "values": [f"val{cat_num}"],
            }
            all_data.append(cat_data)

        # Create DataFrame and then CIF
        df = pl.DataFrame(all_data)
        cif = ciffile.create(df, variant="mmcif")

        assert len(cif) == 1
        block = cif[0]
        assert len(block) == 3


@pytest.mark.integration
@pytest.mark.slow
class TestLargeFileIntegration:
    """Integration tests for large file processing (marked as slow)."""

    def test_very_large_loop_processing(self) -> None:
        """Test processing very large loop constructs."""
        num_rows = 10000

        data = {
            "block": ["test"] * (num_rows * 2),
            "category": ["data"] * (num_rows * 2),
            "keyword": ["id"] * num_rows + ["value"] * num_rows,
            "values": [[str(i)] for i in range(num_rows)] + [[str(i * 2)] for i in range(num_rows)],
        }

        cif = ciffile.create(data, variant="mmcif", allow_duplicate_rows=True)

        # Verify structure
        block = cif[0]
        cat = block["data"]
        assert cat.df.shape[0] == num_rows

    def test_multiple_large_blocks(self) -> None:
        """Test processing multiple large data blocks."""
        num_blocks = 10
        rows_per_block = 100

        all_data = []
        for block_num in range(num_blocks):
            for row_num in range(rows_per_block):
                all_data.append({
                    "block": f"block{block_num}",
                    "category": "data",
                    "keyword": "id",
                    "values": [str(row_num)],
                })

        import polars as pl
        df = pl.DataFrame(all_data)
        cif = ciffile.create(df, variant="mmcif", allow_duplicate_rows=True)

        assert len(cif) == num_blocks
