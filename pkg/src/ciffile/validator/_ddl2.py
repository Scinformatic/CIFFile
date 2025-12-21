"""DDL2 validator."""

from typing import Any, Literal

import polars as pl

from ciffile.structure import CIFFile, CIFBlock, CIFDataCategory
from ._validator import CIFFileValidator
from ._ddl2_model import DDL2Dictionary


class DDL2Validator(CIFFileValidator):
    """DDL2 validator for CIF files."""

    def __init__(self, dictionary: dict) -> None:
        super().__init__(dictionary)
        DDL2Dictionary(**dictionary)  # validate dictionary structure
        dictionary["mandatory_categories"] = mandatory_categories = []
        for category_id, category in dictionary["category"].items():

            category["mandatory_items"] = []

            if category["mandatory"]:
                mandatory_categories.append(category_id)

            category["groups"] = {
                group_id: dictionary["category_group"].get(group_id, {})
                for group_id in category.get("groups", [])
            }

        for item_name, item in dictionary["item"].items():

            item_category_id = item["category"]
            if item["mandatory"]:
                dictionary["category"][item_category_id]["mandatory_items"].append(item_name)

            item["sub_categories"] = {
                sub_cat: dictionary["sub_category"][sub_cat]
                for sub_cat in item.get("sub_categories", [])
            }

            item_type = item["type"]
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

        # Check existence of mandatory items in category
        for mandatory_item_name in catdef["mandatory_items"]:
            if mandatory_item_name not in cat:
                raise ValueError(
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
                itemdef = self.item_def(cat.code, data_item.code)
                data_item.description = itemdef["description"]
                data_item.mandatory = itemdef["mandatory"]
                data_item.default = itemdef.get("default")
                data_item.enum = itemdef.get("enum")
                data_item.dtype = itemdef.get("type")
                data_item.range = itemdef.get("range")
                data_item.unit = itemdef.get("unit")
        return []


def validate_mmcif_category_table(
    table: pl.DataFrame,
    item_defs: dict[str, dict[str, Any]],
) -> pl.DataFrame:
    """Validate a MMCIF category table against category item definitions.

    Parameters
    ----------
    table
        MMCIF category table as a Polars DataFrame.
        Each column corresponds to a data item.
    item_defs
        Dictionary of data item definitions for the category.
        Keys are data item keywords (column names),
        and values are dictionaries with the following key-value pairs:
        - "default" (string): Default value for the data item (as a string),
          or `None` if no default is specified.
        - "enum" (list of strings): List of allowed values for the data item;
          empty when no enumeration is specified.
        - "range" (list of (floats or 2-tuple of floats)): List of allowed ranges for the data item.
          Each range is either a single float value (indicating an exact match)
          or a tuple of two floats (indicating an exclusive minimum and maximum value).
          The allowed range for the data item is the union of all specified ranges.
          An empty list indicates no range restrictions.
        - "type" (dictionary): Data type definition for the data item,
          with keys:
          - "code" (string): Data type code (e.g., "boolean", "float", "int", "text"),
            or `None` if no type is specified.
          - "condition" (string): Condition for the data type; one of:
            - "esd": permits a number string to contain an appended standard
              deviation number enclosed within parentheses, e.g., "4.37(5)".
            - "seq": permits data to be declared as a sequence of values
              separated by a comma <,> or a colon <:>.
              * The sequence v1,v2,v3,... signals that v1, v2, v3, etc.
                are alternative values or the data item.
              * The sequence v1:v2 signals that v1 and v2 are the boundary
                values of a continuous range of values.
            - `None`: no special condition.

              Combinations of alternate and range sequences are permitted,
              e.g., v1,v2:v3,v4.
          - "primitive_code" (string): Primitive data type code; one of:
            - "numb": numerically intererpretable string
            - "char": case-sensitive character or text string
            - "uchar": case-insensitive character or text string
            - `None`: no primitive type specified.
          - "construct" (string): Data type construct (regex).
            When a data value can be defined as a pre-determined sequence of characters,
            or optional characters, or data names (for which the definition is also available),
            it is specified as a construction.
            The rules of construction conform to the the regular expression (REGEX)
            specifications detailed in the IEEE document P1003.2
            Draft 11.2 Sept 1991 (ftp file '/doc/POSIX/1003.2/p121-140').
            Resolved data names for which _item_type_list.construct
            specifications exist are replaced by these constructions,
            otherwise the data name string is not replaced.
            If no construction is specified, the value is `None`.

    Returns
    -------
    Processed MMCIF category table as a Polars DataFrame.
    The procedure works as follows for each data item (column) in the table:
    1. If the item has a default value defined,
       all missing ("?") values in the column are replaced with the default value.
       Otherwise, the item (column) name and the row indices of missing values are collected,
       and missing values are replaced with nulls.
       The rest of the steps only apply to values that are not missing (i.e., not null)
       and not inapplicable (i.e., not ".").
    2. If the item has a type construct defined,
       all values in the column are checked against the construct regex.
       Column names and row indices of values that do not match the construct are collected.

    """
    return

