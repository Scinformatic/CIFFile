# CIFFile

[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A comprehensive Python library for reading, creating, processing, validating, and writing Crystallographic Information Files ([CIF](https://www.iucr.org/resources/cif)), including Protein Data Bank Exchange macromolecular Crystallographic Information Files ([PDBx/mmCIF](https://mmcif.wwpdb.org/docs/user-guide/guide.html)) used by the Worldwide Protein Data Bank ([wwPDB](https://www.wwpdb.org/)).

## Features

- **📖 Read CIF Files**: Parse CIF files from strings, file paths, or file-like objects
- **✏️ Create CIF Files**: Build CIF files from tabular data (Polars, Pandas, dictionaries, etc.)
- **✅ Validate**: Validate CIF files against DDL2 dictionaries
- **💾 Write**: Export CIF files with customizable formatting and styling
- **🔄 Convert**: Transform between different CIF representations
- **🎯 Query**: Access data blocks, save frames, categories, and items with intuitive indexing
- **🐼 DataFrames**: Work seamlessly with Polars and Pandas DataFrames
- **🧪 Two Variants**: Support for both CIF 1.1 and mmCIF formats

## Installation

CIFFile requires Python 3.12 or later and can be installed from PyPI using `pip`:

```bash
pip install ciffile
```

### Dependencies

- [Polars](https://pola.rs/) >= 1.0 (high-performance DataFrame library)
- [Pydantic](https://docs.pydantic.dev/latest/) >= 2.0 (data validation)
- [FileEx](https://github.com/scinformatic/fileex) >= 0.2.10 (file handling utilities)
- [tqdm](https://github.com/tqdm/tqdm) >= 4.0 (progress bars)

## Quick Start

### Reading CIF Files

```python
import ciffile

# Read from file path
cif = ciffile.read("path/to/file.cif")

# Read from string content
cif_content = """
data_example
_item_name  'value'
loop_
_atom_site.id
_atom_site.symbol
1 C
2 N
"""
cif = ciffile.read(cif_content)

# Access data blocks
block = cif["example"]  # or cif[0]

# Access categories
atom_site = block["atom_site"]

# Access data as DataFrame
df = atom_site.df
print(df)
```

### Creating CIF Files

```python
import ciffile
import polars as pl

# Create from dictionary
data = {
	"block": ["my_data"] * 4,
	"category": ["atom_site"] * 2 + ["cell"] * 2,
	"keyword": ["id", "symbol", "length_a", "length_b"],
	"values": [["1", "2"], ["C", "N"], ["10.0"], ["20.0"]],
}
cif = ciffile.create(data, variant="mmcif")

# Write to string
print(cif)

# Write to file
with open("output.cif", "w") as f:
	cif.write(f.write)
```

### Validating CIF Files

```python
# Read dictionary file
dictionary_cif = ciffile.read("mmcif_pdbx_v50.dic")

# Convert to validator dictionary format
validator_dict = dictionary_cif.to_validator_dict(variant="ddl2")

# Create validator using the public API
validator = ciffile.validator(validator_dict)

# Validate and cast data (modifies cif in-place, returns error DataFrame)
errors = validator.validate(cif)
```

## Usage Examples

### Working with Data Blocks

```python
# Iterate over blocks
for block in cif:
	print(f"Block: {block.code}")
	print(f"  Categories: {len(block)}")
	print(f"  Frames: {len(block.frames)}")

# Access multiple blocks
block1, block2 = cif[0, 1]

# Check if block exists
if "my_block" in cif:
	block = cif["my_block"]
```

### Working with Categories

```python
# Get category
category = block["atom_site"]

# Access as DataFrame
df = category.df

# Get item names
print(category.item_names)

# Iterate over items
for item in category:
	print(f"{item.name}: {item.value}")

# Set category keys (for sorting)
category.keys = ["id"]
```

### Working with Save Frames

```python
# Access save frames (for dictionary files)
frames = block.frames

# Get specific frame
frame = frames["atom_site.id"]

# Access frame categories
for category in frame:
	print(category.code)
```

### Customizing Output Format

```python
# Customize writing style
cif.write(
	writer=print,  # or file.write
	# String representations
	bool_true="yes",
	bool_false="no",
	null_str="?",
	empty_str=".",
	# Formatting
	list_style="horizontal",  # or "tabular", "vertical"
	table_style="tabular-horizontal",  # or "horizontal", "tabular-vertical", "vertical"
	space_items=3,
	min_space_columns=2,
	indent=0,
	indent_inner=2,
	delimiter_preference=("single", "double", "semicolon"),
)
```

### Converting to Dictionary Format

```python
# Convert DataFrame to nested dictionary
data_dict = category.to_id_dict(
	ids="id",  # or ["id1", "id2"] for multiple keys
	flat=False,  # nested structure
	single_row="value",  # return value directly for single rows
	multi_row="list",  # return list for multiple rows
)
```

### Extracting Categories Across Blocks

```python
# Extract specific categories from all blocks/frames
categories = cif.category("atom_site", "cell")

# Returns dict of CIFDataCategory objects
atom_site_cat = categories["atom_site"]
cell_cat = categories["cell"]

# DataFrames include block/frame columns for tracking
print(atom_site_cat.df)
```

## Architecture

CIFFile provides a hierarchical structure for CIF data:

```
CIFFile
├── CIFBlock (data_*)
│   ├── CIFDataCategory
│   │   └── CIFDataItem
│   └── CIFBlockFrames (save frames)
│       └── CIFFrame (save_*)
│           └── CIFDataCategory
│               └── CIFDataItem
```

Each level supports:
- **Indexing**: Access by code/name or integer index
- **Iteration**: Loop over contained elements
- **Length**: Count of direct children
- **Membership**: Check existence with `in`
- **DataFrame representation**: Access underlying data as Polars DataFrame

## Supported CIF Variants

### CIF 1.1
- Standard crystallographic CIF format
- Data names without category requirement
- Suitable for small molecule structures

### mmCIF (PDBx/mmCIF)
- Macromolecular CIF format
- Data names must have category.keyword format
- Used by Protein Data Bank
- Supports large biological macromolecules

## File Types

CIFFile distinguishes between:

- **Data Files**: Regular CIF files with data blocks containing scientific data
- **Dictionary Files**: CIF files with save frames defining data item semantics

## Advanced Features

### Parser Options

```python
cif = ciffile.read(
	"file.cif",
	variant="mmcif",  # or "cif1"
	encoding="utf-8",
	case_normalization="lower",  # or "upper", None
	raise_level=2,  # 0: all errors, 1: errors only, 2: fatal only
	col_name_block="block",  # customize column names
	col_name_frame="frame",
	col_name_cat="category",
	col_name_key="keyword",
	col_name_values="values",
)
```

### Isolating File Parts

```python
# Separate data and dictionary sections
parts = cif.part("data", "dict")
data_file = parts["data"]
dict_file = parts["dict"]

# Or just one part
data_only = cif.part("data")
```


### Converting Back to Strings

After validating and working with typed data, you can convert columns back to CIF string format for writing:

```python
# Convert typed columns back to strings (in-place modification)
validator.values_to_str(cif)
```


## Documentation

For more detailed examples and tutorials, see:
- [QUICKSTART.ipynb](./QUICKSTART.ipynb) - Interactive Jupyter notebook with examples
- Inline documentation - All classes and functions have comprehensive docstrings
- [Official CIF Specification](https://www.iucr.org/resources/cif/spec/version1.1)
- [mmCIF Documentation](https://mmcif.wwpdb.org/)

## Testing

Run the test suite with pytest:

```bash
cd test
pytest -v
```

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## References

### Official CIF Resources
- [IUCr CIF Resources](https://www.iucr.org/resources/cif)
- [CIF Specification v1.1](https://www.iucr.org/resources/cif/spec/version1.1)
- [CIF Dictionaries](https://www.iucr.org/resources/cif/dictionaries)
- [CIF DDL](https://www.iucr.org/resources/cif/ddl)
- [Online CIF Validator](https://checkcif.iucr.org/)

### mmCIF Resources
- [PDBx/mmCIF Dictionary](https://mmcif.wwpdb.org/dictionaries/)
- [mmCIF User Guide](https://mmcif.wwpdb.org/docs/user-guide/guide.html)
- [wwPDB](https://www.wwpdb.org/)

### Related Python Packages
- [mmCIF Core Access Library (RCSB)](https://github.com/rcsb/py-mmcif)
- [PDBeCIF (PDBe)](https://github.com/PDBeurope/pdbecif)
- [BioPython](https://biopython.org/)
- [BioPandas](https://github.com/BioPandas/biopandas)
- [Biotite](https://www.biotite-python.org/)

## Acknowledgments

This library implements the CIF 1.1 specification as defined by the International Union of Crystallography (IUCr) and supports the PDBx/mmCIF format used by the Worldwide Protein Data Bank (wwPDB).

---

**Note**: Currently only CIF Version 1.1 is supported. CIF Version 2.0 support is planned for future releases.
