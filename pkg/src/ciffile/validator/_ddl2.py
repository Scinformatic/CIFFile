"""DDL2 validator."""

from typing import Any, Sequence, Literal

import polars as pl

from ciffile.structure import CIFFile, CIFBlock, CIFDataCategory
from ._validator import CIFFileValidator
from ._ddl2_model import DDL2Dictionary


class DDL2Validator(CIFFileValidator):
    """DDL2 validator for CIF files."""

    def __init__(
        self,
        dictionary: dict,
        binary_enum_to_bool: bool = True,
        bool_true: Sequence[str] = ("yes", "y", "true"),
        bool_false: Sequence[str] = ("no", "n", "false"),
    ) -> None:
        super().__init__(dictionary)
        DDL2Dictionary(**dictionary)  # validate dictionary structure
        dictionary["mandatory_categories"] = mandatory_categories = []
        for category_id, category in dictionary["category"].items():

            category["mandatory_items"] = []

            if category["mandatory"]:
                mandatory_categories.append(category_id)

            category["groups"] = {
                group_id: dictionary["category_group"][group_id]
                for group_id in category.get("groups", [])
            }

        self._bool_true = set(bool_true)
        self._bool_false = set(bool_false)

        bool_enums = self._bool_true | self._bool_false

        for item_name, item in dictionary["item"].items():

            # Check mandatory items and add to category definition
            if item["mandatory"]:
                dictionary["category"][item["category"]]["mandatory_items"].append(item_name)

            item["sub_category"] = {
                sub_cat: dictionary["sub_category"][sub_cat]
                for sub_cat in item.get("sub_category", [])
            }

            item_type = item["type"]

            if binary_enum_to_bool and "enumeration" in item and all(enum in bool_enums for enum in item["enumeration"].values()):
                item_type = item["type"] = "boolean"
                item.pop("enumeration")


            item_type_info = dictionary["item_type"][item_type]
            item["type_primitive"] = item_type_info["primitive"]
            item["type_regex"] = item_type_info["regex"]
            item["type_detail"] = item_type_info.get("detail")
        return

    @property
    def dict_title(self) -> str | None:
        """Title of the dictionary."""
        return self._dict["title"]

    @property
    def dict_description(self) -> str | None:
        """Description of the dictionary."""
        return self._dict["description"]

    @property
    def dict_version(self) -> str | None:
        """Version of the dictionary."""
        return self._dict["version"]

    def validate(
        self,
        file: CIFFile | CIFBlock | CIFDataCategory,
        add_category_info: bool = True,
        add_item_info: bool = True,
    ) -> list:
        def validate_category(
            category: CIFDataCategory,
            parent_block_code: str | None = None,
            parent_frame_code: str | None = None,
        ) -> list:
            return self._validate_category(
                category,
                parent_block_code=parent_block_code,
                parent_frame_code=parent_frame_code,
                add_category_info=add_category_info,
                add_item_info=add_item_info,
            )

        if isinstance(file, CIFDataCategory):
            return validate_category(file)

        blocks = [file] if isinstance(file, CIFBlock) else file
        errs = []
        for block in blocks:
            for mandatory_cat in self._dict["mandatory_categories"]:
                if mandatory_cat not in block:
                    errs.append(
                        f"DDL2Validator: Missing mandatory category "
                        f"'{mandatory_cat}' in block '{block.code}'."
                    )
            for frame in block.frames:
                for frame_category in frame:
                    frame_errs = validate_category(
                        frame_category, parent_block_code=block.code, parent_frame_code=frame.code
                    )
                    errs.extend(frame_errs)
            for block_category in block:
                block_errs = validate_category(block_category, parent_block_code=block.code)
                errs.extend(block_errs)
        return errs

    def _validate_category(
        self,
        cat: CIFDataCategory,
        parent_block_code: str | None,
        parent_frame_code: str | None,
        add_category_info: bool,
        add_item_info: bool,
    ) -> list:
        try:
            catdef = self["category"][cat.code]
        except KeyError:
            raise ValueError(
                f"DDL2Validator: Category '{cat.code}' "
                f"not defined in DDL2 dictionary."
            )

        errs = []

        # Check existence of mandatory items in category
        for mandatory_item_name in catdef["mandatory_items"]:
            if mandatory_item_name not in cat.item_names:
                errs.append(
                    f"DDL2Validator: Missing mandatory data item "
                    f"'{mandatory_item_name}' in category '{cat.code}'."
                )

        # Add category info
        if add_category_info:
            cat.description = catdef["description"]
            cat.groups = catdef["groups"]
            cat.keys = catdef["keys"]

        # Add item info
        if add_item_info:
            for data_item in cat:
                itemdef = self["item"][data_item.name]
                data_item.description = itemdef["description"]
                data_item.mandatory = itemdef["mandatory"]
                data_item.default = itemdef.get("default")
                data_item.enum = itemdef.get("enumeration")
                data_item.dtype = itemdef.get("type")
                data_item.range = itemdef.get("range")
                data_item.unit = itemdef.get("units")
        return []


def validate_mmcif_category_table(
    table: pl.DataFrame,
    item_defs: dict[str, dict[str, Any]],
    case_normalization: Literal["lower", "upper"] | None = "lower",
) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    """Validate an mmCIF category table against category item definitions.

    Parameters
    ----------
    table
        mmCIF category table as a Polars DataFrame.
        Each column corresponds to a data item.
    item_defs
        Dictionary of data item definitions for the category.
        Keys are data item keywords (column names),
        and values are dictionaries with the following key-value pairs:
        - "default" (string | None): Default value for the data item (as a string),
          or `None` if no default is specified.
        - "enum" (list of strings | None): List of allowed values for the data item;
          or `None` if no enumeration is specified.
        - "range" (list of 2-tuples of floats or None | None): List of allowed ranges for the data item.
          Each range is a 2-tuple indicating an exclusive minimum and maximum value, respectively.
          A value of `None` for minimum or maximum indicates no bound in that direction.
          If both minimum and maximum are the same non-None float value,
          it indicates that only that exact value is allowed.
          The allowed range for the data item is the union of all specified ranges.
          If `None`, no range is specified.
        - "caster" (function): Data type casting function for the data item.
          The function takes a Polars expression (column) as input
          and returns a list of one or several Polars expressions:
          the first expression yields the casted data column,
          this may be a nested structure such as a List, Array, List of Array, etc.
          Any subsequent expressions yield auxiliary columns derived from the data column,
          such as estimated standard deviations (ESDs) for floating-point data items.
        - "type_primitive" ({"numb", "char", "uchar"}): Primitive data type code; one of:
          - "numb": numerically intererpretable string
          - "char": case-sensitive character or text string
          - "uchar": case-insensitive character or text string
        - "type_regex" (string): Data type construct (regex).
    case_normalization
        Case normalization for "uchar" (case-insensitive character) data items.
        If "lower", all values are converted to lowercase.
        If "upper", all values are converted to uppercase.
        If `None`, no case normalization is performed.

    Returns
    -------
    validated_table
        Processed mmCIF category table as a Polars DataFrame.
    validation_errors
        List of validation error dictionaries.
        Each dictionary contains the following key-value pairs:
        - "item" (string): Data item (column) name.
        - "row_indices" (list of int): List of row indices (0-based) with validation errors for the data item.
        - "error_type" (string): Type of validation error.
          One of: "missing_value", "construct_mismatch", "enum_violation",
          "range_violation", "auxiliary_mismatch".

    Notes
    -----
    The procedure works as follows for each data item (column) in the table:
    1. If the item has a default value defined,
       all missing ("?") values in the column are replaced with the default value.
       Otherwise, the item (column) name and the row indices of missing values are collected,
       and missing values are replaced with nulls.
    2. All values in the column that are not `null` or "." (i.e., not missing or inapplicable)
       are checked against the construct regex.
       Column names and row indices of values that do not match the construct are collected.
    3. If the data item is of primitive type "uchar"
       and case normalization is specified,
       all values in the column are converted to the specified case.
    4. The data is converted to the appropriate data type
       using the caster function defined for the data item.
       This also converts any inapplicable (".") values to nulls/NaNs/empty strings
       as appropriate for the data type.
    5. If the item has an enumeration defined,
       all (innermost) values in the column that are not null/NaN/empty are checked against the enumeration,
       and column names and row indices of values not in the enumeration are collected.
       If all values are in the enumeration, the column is replaced
       with a categorical column with categories defined by the enumeration.
       If the data item is of primitive type "uchar"
       and case normalization is specified,
       the enumeration values are normalized to the specified case before checking.
    6. If the item has a range defined,
       all (innermost) values in the column are checked against the range,
       and column names and row indices of values outside the range are collected.
    7. If the caster function produces auxiliary columns,
       these are added to the output DataFrame with column names
       specified by the caster function.
       If the produced auxiliary columns already exist in the input table,
       - for rows where the input auxiliary column is null/NaN,
         the value from the caster-produced column is used,
       - for rows where the input auxiliary column is not null/NaN,
         it is compared with the caster-produced column,
         and any discrepancies are collected.
    """
    return

