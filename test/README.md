# Test Suite for CIFFile

This directory contains the comprehensive test suite for the CIFFile package.

## Structure

```
test/
├── conftest.py              # Pytest configuration and fixtures
├── pytest.ini               # Pytest settings and markers
├── test_reader.py           # Tests for CIF file reading
├── test_creator.py          # Tests for CIF file creation
├── test_writer.py           # Tests for CIF file writing
├── test_parser.py           # Tests for CIF parser
├── test_structure.py        # Tests for CIF structure classes
├── test_utils.py            # Tests for utility functions
├── test_integration.py      # Integration tests
└── README.md                # This file
```

## Running Tests

### Run All Tests

```bash
pytest
```

### Run with Verbose Output

```bash
pytest -v
```

### Run Specific Test File

```bash
pytest test_reader.py
```

### Run Specific Test Class

```bash
pytest test_reader.py::TestCIFReader
```

### Run Specific Test Method

```bash
pytest test_reader.py::TestCIFReader::test_read_from_string
```

### Run Tests by Marker

```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Run only parser tests
pytest -m parser

# Run only fast tests (exclude slow tests)
pytest -m "not slow"

# Combine markers
pytest -m "unit and parser"
```

## Test Markers

The test suite uses the following markers:

- `unit`: Unit tests for individual components
- `integration`: Integration tests for complete workflows
- `slow`: Tests that take longer to run
- `parser`: Tests for parser functionality
- `creator`: Tests for creator functionality
- `writer`: Tests for writer functionality
- `validator`: Tests for validator functionality
- `structure`: Tests for structure classes

## Test Coverage

### Modules Tested

1. **Reader** (`test_reader.py`)
   - Reading from strings, files, and file objects
   - CIF 1.1 and mmCIF variants
   - Case normalization
   - Custom column names
   - Multi-block files
   - Dictionary files with save frames
   - Error handling

2. **Creator** (`test_creator.py`)
   - Creating from dictionaries, Polars/Pandas DataFrames
   - CIF variants
   - Validation options
   - Custom column names
   - Multiple blocks
   - Save frames
   - Loop and single value data

3. **Writer** (`test_writer.py`)
   - Writing to strings, files, and collectors
   - List and table styles
   - Custom formatting options
   - Boolean and null value representations
   - Spacing and indentation
   - Roundtrip consistency

4. **Parser** (`test_parser.py`)
   - Single values and loops
   - Quoted strings and text fields
   - Numeric values
   - Comments
   - Save frames
   - mmCIF category syntax
   - Case sensitivity
   - Special characters
   - Error handling

5. **Structure** (`test_structure.py`)
   - CIFFile, CIFBlock, CIFDataCategory, CIFDataItem classes
   - Indexing and iteration
   - DataFrame representations
   - String representations
   - Metadata (description, unit, keys)

6. **Utilities** (`test_utils.py`)
   - Whitespace normalization
   - DataFrame conversion
   - Category extraction
   - File part isolation

7. **Integration** (`test_integration.py`)
   - Complete read-write-read cycles
   - File I/O workflows
   - DataFrame manipulation
   - Multi-block processing
   - Large file handling
   - Error recovery

## Fixtures

The test suite provides several fixtures in `conftest.py`:

- `sample_cif1_content`: CIF 1.1 format content
- `sample_mmcif_content`: mmCIF format content
- `sample_dict_file_content`: Dictionary file with save frames
- `sample_multiblock_content`: Multi-block CIF file
- `sample_dataframe`: Polars DataFrame for CIF creation
- `temp_cif_file`: Temporary CIF file for I/O tests
- `sample_cif_file`: Parsed CIF file object
- `sample_cif_block`: CIF data block
- `sample_category`: CIF data category

## Requirements

The tests require:

- pytest >= 7.0
- polars >= 1.0
- pandas >= 2.0 (for Pandas DataFrame tests)
- The CIFFile package and its dependencies

## Test Philosophy

The test suite follows these principles:

1. **Comprehensive Coverage**: Tests cover all major functionality and edge cases
2. **Isolated Tests**: Each test is independent and can run in any order
3. **Clear Documentation**: Every test has a descriptive docstring
4. **Type Hints**: All test functions include type hints
5. **Fixtures**: Reusable test data through pytest fixtures
6. **Markers**: Organized by functionality and speed for selective running
7. **Integration**: Both unit and integration tests for complete coverage

## Adding New Tests

When adding new tests:

1. Choose the appropriate test file based on functionality
2. Add descriptive docstrings
3. Use type hints for parameters and return values
4. Apply appropriate markers (`@pytest.mark.unit`, etc.)
5. Use fixtures for common test data
6. Keep tests focused on a single aspect of functionality
7. Include both positive and negative test cases

## Continuous Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: |
    pytest -v --cov=ciffile --cov-report=xml
```

## Known Limitations

- Some tests may require external dependencies (pdbapi for downloading test files)
- Large file tests are marked as `slow` and can be skipped for faster test runs
- Validation tests require dictionary files which may not be included in the test fixtures
