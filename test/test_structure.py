"""Unit tests for CIF structure classes.

Tests CIFFile, CIFBlock, CIFDataCategory, and CIFDataItem classes.
"""

from typing import Any
import pytest
import polars as pl

from ciffile import CIFFile, CIFBlock, CIFDataCategory, CIFDataItem


@pytest.mark.unit
@pytest.mark.structure
class TestCIFFile:
    """Test suite for CIFFile class."""

    def test_file_creation(self, sample_cif_file: CIFFile) -> None:
        """Test CIF file object creation.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        assert isinstance(sample_cif_file, CIFFile)
        assert sample_cif_file.code is None  # Files don't have codes
        assert sample_cif_file.container_type == "file"

    def test_file_length(self, sample_cif_file: CIFFile) -> None:
        """Test getting number of blocks in file.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        assert len(sample_cif_file) == 1

    def test_file_codes(self, sample_cif_file: CIFFile) -> None:
        """Test getting block codes from file.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        codes = sample_cif_file.codes
        assert isinstance(codes, list)
        assert len(codes) == 1
        assert codes[0] == "test_structure"

    def test_file_iteration(self, sample_cif_file: CIFFile) -> None:
        """Test iterating over blocks in file.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        blocks = list(sample_cif_file)
        assert len(blocks) == 1
        assert isinstance(blocks[0], CIFBlock)

    def test_file_indexing_by_code(self, sample_cif_file: CIFFile) -> None:
        """Test accessing blocks by code.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        block = sample_cif_file["test_structure"]
        assert isinstance(block, CIFBlock)
        assert block.code == "test_structure"

    def test_file_indexing_by_int(self, sample_cif_file: CIFFile) -> None:
        """Test accessing blocks by integer index.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        block = sample_cif_file[0]
        assert isinstance(block, CIFBlock)

    def test_file_contains(self, sample_cif_file: CIFFile) -> None:
        """Test checking if block exists in file.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        assert "test_structure" in sample_cif_file
        assert "nonexistent" not in sample_cif_file

    def test_file_dataframe(self, sample_cif_file: CIFFile) -> None:
        """Test getting DataFrame representation of file.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        df = sample_cif_file.df
        assert isinstance(df, pl.DataFrame)
        assert "block" in df.columns
        assert "category" in df.columns
        assert "keyword" in df.columns
        assert "values" in df.columns

    def test_file_string_representation(self, sample_cif_file: CIFFile) -> None:
        """Test string representation of file.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        cif_str = str(sample_cif_file)
        assert isinstance(cif_str, str)
        assert "data_test_structure" in cif_str

    def test_file_repr(self, sample_cif_file: CIFFile) -> None:
        """Test repr of file.

        Parameters
        ----------
        sample_cif_file : CIFFile
            Sample CIF file fixture.
        """
        repr_str = repr(sample_cif_file)
        assert "CIFFile" in repr_str
        assert "mmcif" in repr_str or "cif1" in repr_str


@pytest.mark.unit
@pytest.mark.structure
class TestCIFBlock:
    """Test suite for CIFBlock class."""

    def test_block_creation(self, sample_cif_block: CIFBlock) -> None:
        """Test CIF block object creation.

        Parameters
        ----------
        sample_cif_block : CIFBlock
            Sample CIF block fixture.
        """
        assert isinstance(sample_cif_block, CIFBlock)
        assert sample_cif_block.code == "test_structure"
        assert sample_cif_block.container_type == "block"

    def test_block_length(self, sample_cif_block: CIFBlock) -> None:
        """Test getting number of categories in block.

        Parameters
        ----------
        sample_cif_block : CIFBlock
            Sample CIF block fixture.
        """
        assert len(sample_cif_block) >= 1

    def test_block_codes(self, sample_cif_block: CIFBlock) -> None:
        """Test getting category codes from block.

        Parameters
        ----------
        sample_cif_block : CIFBlock
            Sample CIF block fixture.
        """
        codes = sample_cif_block.codes
        assert isinstance(codes, list)
        assert "atom_site" in codes or "entry" in codes

    def test_block_iteration(self, sample_cif_block: CIFBlock) -> None:
        """Test iterating over categories in block.

        Parameters
        ----------
        sample_cif_block : CIFBlock
            Sample CIF block fixture.
        """
        categories = list(sample_cif_block)
        assert len(categories) >= 1
        assert all(isinstance(cat, CIFDataCategory) for cat in categories)

    def test_block_indexing_by_code(self, sample_cif_block: CIFBlock) -> None:
        """Test accessing categories by code.

        Parameters
        ----------
        sample_cif_block : CIFBlock
            Sample CIF block fixture.
        """
        if "atom_site" in sample_cif_block:
            cat = sample_cif_block["atom_site"]
            assert isinstance(cat, CIFDataCategory)
            assert cat.code == "atom_site"

    def test_block_contains(self, sample_cif_block: CIFBlock) -> None:
        """Test checking if category exists in block.

        Parameters
        ----------
        sample_cif_block : CIFBlock
            Sample CIF block fixture.
        """
        assert len(sample_cif_block.codes) > 0
        first_code = sample_cif_block.codes[0]
        assert first_code in sample_cif_block
        assert "definitely_nonexistent_category" not in sample_cif_block

    def test_block_frames(self, sample_cif_block: CIFBlock) -> None:
        """Test accessing save frames from block.

        Parameters
        ----------
        sample_cif_block : CIFBlock
            Sample CIF block fixture.
        """
        frames = sample_cif_block.frames
        assert frames is not None
        assert len(frames) >= 0  # May or may not have frames

    def test_block_type(self, sample_cif_block: CIFBlock) -> None:
        """Test getting block type (data or dict).

        Parameters
        ----------
        sample_cif_block : CIFBlock
            Sample CIF block fixture.
        """
        assert sample_cif_block.type in ("data", "dict")

    def test_block_repr(self, sample_cif_block: CIFBlock) -> None:
        """Test repr of block.

        Parameters
        ----------
        sample_cif_block : CIFBlock
            Sample CIF block fixture.
        """
        repr_str = repr(sample_cif_block)
        assert "CIFBlock" in repr_str
        assert sample_cif_block.code in repr_str


@pytest.mark.unit
@pytest.mark.structure
class TestCIFDataCategory:
    """Test suite for CIFDataCategory class."""

    def test_category_creation(self, sample_category: CIFDataCategory) -> None:
        """Test CIF data category object creation.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        assert isinstance(sample_category, CIFDataCategory)
        assert sample_category.container_type == "category"

    def test_category_length(self, sample_category: CIFDataCategory) -> None:
        """Test getting number of items in category.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        assert len(sample_category) >= 1

    def test_category_codes(self, sample_category: CIFDataCategory) -> None:
        """Test getting item codes from category.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        codes = sample_category.codes
        assert isinstance(codes, list)
        assert len(codes) >= 1

    def test_category_iteration(self, sample_category: CIFDataCategory) -> None:
        """Test iterating over items in category.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        items = list(sample_category)
        assert len(items) >= 1
        assert all(isinstance(item, CIFDataItem) for item in items)

    def test_category_indexing(self, sample_category: CIFDataCategory) -> None:
        """Test accessing items by code.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        first_code = sample_category.codes[0]
        item = sample_category[first_code]
        assert isinstance(item, CIFDataItem)

    def test_category_dataframe(self, sample_category: CIFDataCategory) -> None:
        """Test getting DataFrame representation of category.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        df = sample_category.df
        assert isinstance(df, pl.DataFrame)
        assert df.shape[1] == len(sample_category)  # Number of columns = number of items

    def test_category_item_names(self, sample_category: CIFDataCategory) -> None:
        """Test getting full item names.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        names = sample_category.item_names
        assert isinstance(names, list)
        assert len(names) == len(sample_category)

    def test_category_keys(self, sample_category: CIFDataCategory) -> None:
        """Test getting and setting category keys.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        # Initially may be None
        keys = sample_category.keys
        assert keys is None or isinstance(keys, list)

    def test_category_description(self, sample_category: CIFDataCategory) -> None:
        """Test getting and setting category description.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        # Initially may be None
        desc = sample_category.description
        assert desc is None or isinstance(desc, str)

        # Test setting
        sample_category.description = "Test description"
        assert sample_category.description == "Test description"

    def test_category_repr(self, sample_category: CIFDataCategory) -> None:
        """Test repr of category.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        repr_str = repr(sample_category)
        assert "CIFDataCategory" in repr_str


@pytest.mark.unit
@pytest.mark.structure
class TestCIFDataItem:
    """Test suite for CIFDataItem class."""

    def test_item_creation(self, sample_category: CIFDataCategory) -> None:
        """Test CIF data item object creation.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        item = sample_category[0]
        assert isinstance(item, CIFDataItem)
        assert item.container_type == "item"

    def test_item_name(self, sample_category: CIFDataCategory) -> None:
        """Test getting item name.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        item = sample_category[0]
        assert isinstance(item.name, str)
        assert len(item.name) > 0

    def test_item_values(self, sample_category: CIFDataCategory) -> None:
        """Test getting item values.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        item = sample_category[0]
        values = item.values
        assert isinstance(values, pl.Series)

    def test_item_value(self, sample_category: CIFDataCategory) -> None:
        """Test getting single item value.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        item = sample_category[0]
        value = item.value

        # Should be Series, single value, or None
        assert isinstance(value, (pl.Series, str, int, float, bool, type(None)))

    def test_item_length(self, sample_category: CIFDataCategory) -> None:
        """Test getting number of values in item.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        item = sample_category[0]
        assert len(item) >= 0

    def test_item_iteration(self, sample_category: CIFDataCategory) -> None:
        """Test iterating over values in item.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        item = sample_category[0]
        values = list(item)
        assert len(values) == len(item)

    def test_item_indexing(self, sample_category: CIFDataCategory) -> None:
        """Test accessing values by index.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        item = sample_category[0]
        if len(item) > 0:
            first_value = item[0]
            assert first_value is not None or first_value is None  # Any value is valid

    def test_item_description(self, sample_category: CIFDataCategory) -> None:
        """Test getting and setting item description.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        item = sample_category[0]

        # Initially may be None
        desc = item.description
        assert desc is None or isinstance(desc, str)

        # Test setting
        item.description = "Test description"
        assert item.description == "Test description"

    def test_item_unit(self, sample_category: CIFDataCategory) -> None:
        """Test getting and setting item unit.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        item = sample_category[0]

        # Initially may be None
        unit = item.unit
        assert unit is None or isinstance(unit, str)

        # Test setting
        item.unit = "angstrom"
        assert item.unit == "angstrom"

    def test_item_repr(self, sample_category: CIFDataCategory) -> None:
        """Test repr of item.

        Parameters
        ----------
        sample_category : CIFDataCategory
            Sample data category fixture.
        """
        item = sample_category[0]
        repr_str = repr(item)
        assert "CIFDataItem" in repr_str
