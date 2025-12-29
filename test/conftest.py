"""Comprehensive test suite for the CIFFile package.

This module provides fixtures, utilities, and configuration for testing
the CIF file parser, creator, and validator functionality.
"""

from typing import Generator, Any
import tempfile
from pathlib import Path

import pytest
import polars as pl

import ciffile
from ciffile import CIFFile, CIFBlock, CIFDataCategory, CIFDataItem


# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def sample_cif1_content() -> str:
    """Sample CIF 1.1 format content for testing.

    Returns
    -------
    str
        A simple CIF 1.1 file content with single values and a loop.
    """
    return """
data_test_block
_single_item_1  'value1'
_single_item_2  10.5
_single_item_3  ?

loop_
_loop_item_1
_loop_item_2
_loop_item_3
'row1_col1'  1.0  100
'row2_col1'  2.0  200
'row3_col1'  3.0  300
"""


@pytest.fixture
def sample_mmcif_content() -> str:
    """Sample mmCIF format content for testing.

    Returns
    -------
    str
        A simple mmCIF file content with categories and loops.
    """
    return """
data_test_structure
_entry.id  'TEST'
_entry.title  'Test Structure'

loop_
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
1  C  10.0  20.0  30.0
2  N  11.0  21.0  31.0
3  O  12.0  22.0  32.0

loop_
_cell.length_a
_cell.length_b
_cell.length_c
10.5  20.5  30.5
"""


@pytest.fixture
def sample_dict_file_content() -> str:
    """Sample CIF dictionary file with save frames for testing.

    Returns
    -------
    str
        A CIF dictionary file with save frames.
    """
    return """
data_test_dictionary

save_test_category
    _category.description  'Test category description'
    _category.id  test_category
    _category.mandatory_code  no

    loop_
    _category_key.name
    'test_category.id'
save_

save_test_category.id
    _item.name  'test_category.id'
    _item.category_id  test_category
    _item.mandatory_code  yes
    _item_type.code  code
save_

save_test_category.value
    _item.name  'test_category.value'
    _item.category_id  test_category
    _item.mandatory_code  no
    _item_type.code  text
save_
"""


@pytest.fixture
def sample_multiblock_content() -> str:
    """Sample CIF file with multiple data blocks.

    Returns
    -------
    str
        A CIF file with multiple data blocks.
    """
    return """
data_block_1
_item_a  'value_a1'
_item_b  'value_b1'

data_block_2
_item_a  'value_a2'
_item_b  'value_b2'

data_block_3
_item_a  'value_a3'
_item_b  'value_b3'
"""


@pytest.fixture
def sample_dataframe() -> pl.DataFrame:
    """Sample Polars DataFrame for creating CIF files.

    Returns
    -------
    pl.DataFrame
        A DataFrame suitable for creating a CIF file.
    """
    return pl.DataFrame({
        "block": ["test_data"] * 5,
        "category": ["cat1"] * 3 + ["cat2"] * 2,
        "keyword": ["key1", "key2", "key3", "key4", "key5"],
        "values": [["val1"], ["val2"], ["val3"], ["10", "20"], ["30", "40"]],
    })


@pytest.fixture
def temp_cif_file(sample_mmcif_content: str) -> Generator[Path, None, None]:
    """Create a temporary CIF file for testing file I/O.

    Parameters
    ----------
    sample_mmcif_content : str
        Content to write to the temporary file.

    Yields
    ------
    Path
        Path to the temporary CIF file.
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False) as f:
        f.write(sample_mmcif_content)
        temp_path = Path(f.name)

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def sample_cif_file(sample_mmcif_content: str) -> CIFFile:
    """Create a CIF file object from sample content.

    Parameters
    ----------
    sample_mmcif_content : str
        Sample mmCIF content.

    Returns
    -------
    CIFFile
        Parsed CIF file object.
    """
    return ciffile.read(sample_mmcif_content)


@pytest.fixture
def sample_cif_block(sample_cif_file: CIFFile) -> CIFBlock:
    """Get the first data block from a sample CIF file.

    Parameters
    ----------
    sample_cif_file : CIFFile
        Sample CIF file.

    Returns
    -------
    CIFBlock
        First data block from the CIF file.
    """
    return sample_cif_file[0]


@pytest.fixture
def sample_category(sample_cif_block: CIFBlock) -> CIFDataCategory:
    """Get a sample data category from a CIF block.

    Parameters
    ----------
    sample_cif_block : CIFBlock
        Sample CIF block.

    Returns
    -------
    CIFDataCategory
        A data category from the block.
    """
    return sample_cif_block["atom_site"]


# ============================================================================
# Test Utilities
# ============================================================================


def assert_cif_equal(cif1: CIFFile, cif2: CIFFile) -> None:
    """Assert that two CIF files are equal.

    Parameters
    ----------
    cif1 : CIFFile
        First CIF file.
    cif2 : CIFFile
        Second CIF file.

    Raises
    ------
    AssertionError
        If the CIF files are not equal.
    """
    assert len(cif1) == len(cif2), "Different number of blocks"
    assert cif1.codes == cif2.codes, "Different block codes"

    for code in cif1.codes:
        assert_block_equal(cif1[code], cif2[code])


def assert_block_equal(block1: CIFBlock, block2: CIFBlock) -> None:
    """Assert that two CIF blocks are equal.

    Parameters
    ----------
    block1 : CIFBlock
        First CIF block.
    block2 : CIFBlock
        Second CIF block.

    Raises
    ------
    AssertionError
        If the blocks are not equal.
    """
    assert block1.code == block2.code, "Different block codes"
    assert len(block1) == len(block2), "Different number of categories"
    assert set(block1.codes) == set(block2.codes), "Different category codes"

    for code in block1.codes:
        assert_category_equal(block1[code], block2[code])


def assert_category_equal(cat1: CIFDataCategory, cat2: CIFDataCategory) -> None:
    """Assert that two CIF data categories are equal.

    Parameters
    ----------
    cat1 : CIFDataCategory
        First data category.
    cat2 : CIFDataCategory
        Second data category.

    Raises
    ------
    AssertionError
        If the categories are not equal.
    """
    assert cat1.code == cat2.code, "Different category codes"
    assert cat1.df.shape == cat2.df.shape, "Different DataFrame shapes"
    assert cat1.df.columns == cat2.df.columns, "Different column names"

    # Compare DataFrames
    df1_sorted = cat1.df.sort(cat1.df.columns)
    df2_sorted = cat2.df.sort(cat2.df.columns)
    assert df1_sorted.equals(df2_sorted, null_equal=True), "Different DataFrame content"
