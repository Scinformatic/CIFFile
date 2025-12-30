"""DDL2 validator."""

from __future__ import annotations

from typing import Any, Sequence, Literal, Callable, TYPE_CHECKING
from dataclasses import dataclass

import polars as pl


from .._base import CIFFileValidator
from ._input_schema import DDL2Dictionary
from ._caster import Caster, CastPlan
from ._stringifier import Stringifier, pick_bool_enum_pair

if TYPE_CHECKING:
    from ciffile.structure import CIFFile, CIFBlock, CIFDataCategory
    from ciffile.typing import DataTypeLike


class DDL2Validator(CIFFileValidator):
    """DDL2 validator for CIF files.

    Parameters
    ----------
    dictionary
        DDL2 dictionary metadata.
    """

    def __init__(self, dictionary: dict) -> None:
        super().__init__(dictionary)
        DDL2Dictionary(**dictionary)  # validate dictionary structure

        # Initialize list of mandatory categories
        dictionary["mandatory_categories"] = mandatory_categories = []

        # Preprocess category definitions
        for category_id, category in dictionary["category"].items():

            # Add mandatory categories to list
            if category["mandatory"]:
                mandatory_categories.append(category_id)

            # Replace list of group IDs with dictionary of group IDs to group definitions
            category["groups"] = {
                group_id: dictionary["category_group"][group_id]
                for group_id in category.get("groups", [])
            }

            # Initialize list of mandatory items in category
            category["mandatory_items"] = []

        # Preprocess item definitions
        for item_name, item in dictionary["item"].items():

            # Check mandatory items and add to category definition
            if item["mandatory"]:
                dictionary["category"][item["category"]]["mandatory_items"].append(item_name)

            # Replace list of sub-category IDs with dictionary of sub-category definitions
            item["sub_category"] = {
                sub_cat: dictionary["sub_category"][sub_cat]
                for sub_cat in item.get("sub_category", [])
            }

            # Add type information from item_type definitions
            item_type = item["type"]
            item_type_info = dictionary["item_type"][item_type]
            item["type_primitive"] = item_type_info["primitive"]
            item["type_regex"] = _normalize_for_rust_regex(item_type_info["regex"])
            item["type_detail"] = item_type_info.get("detail")

        self._caster: Caster = Caster()
        self._curr_block_code: str | None = None
        self._curr_frame_code: str | None = None
        self._curr_category_code: str | None = None

        self._curr_item_defs: dict[str, dict[str, Any]] = {}
        """Current item definitions for the category being validated."""

        self._add_category_info: bool = True
        self._add_item_info: bool = True
        self._uchar_case_normalization: Literal["lower", "upper"] | None = "lower"
        self._enum_to_bool: bool = True
        self._enum_true: set[str] = {"yes", "y", "true"}
        self._enum_false: set[str] = {"no", "n", "false"}
        self._errs: list[dict[str, Any]] = []

        # Parameters for `self.values_to_str()`;
        # these are re-set on each call to that method.
        self._stringify_esd_col_suffix: str = "_esd_digits"
        self._stringify_enum_true_set: set[str] = set()
        self._stringify_enum_false_set: set[str] = set()
        self._stringify_enum_bool_set: set[str] = set()
        self._stringify_drop_esd_columns: bool = True
        self._stringify_uchar_case_normalization: Literal["lower", "upper"] | None = None
        self._stringifier: Stringifier = Stringifier()
        return

    def validate(
        self,
        file: CIFFile | CIFBlock | CIFDataCategory,
        *,
        # Casting options
        esd_col_suffix: str = "_esd_digits",
        dtype_float: DataTypeLike = pl.Float64,
        dtype_int: DataTypeLike = pl.Int64,
        cast_strict: bool = True,
        bool_true: Sequence[str] = ("YES",),
        bool_false: Sequence[str] = ("NO",),
        bool_strip: bool = True,
        bool_case_insensitive: bool = True,
        datetime_time_zone: str | None = None,
        uchar_case_normalization: Literal["lower", "upper"] | None = "lower",
        # Enum options
        enum_to_bool: bool = True,
        enum_true: Sequence[str] = ("yes", "y", "true"),
        enum_false: Sequence[str] = ("no", "n", "false"),
        # Info options
        add_category_info: bool = True,
        add_item_info: bool = True,
    ) -> pl.DataFrame:
        """Validate a CIF file, data block, or category against the DDL2 dictionary.

        Parameters
        ----------
        file
            CIF file, data block, or data category to validate.
        esd_col_suffix
            Suffix for estimated standard deviation (ESD) columns.
            When a (float-based) data item has associated ESD,
            the caster will produce an additional column
            with this suffix added to the original column name.
            The original column will then contain the main data values,
            while the ESD column contains the corresponding ESD values as integers.
            The suffix should not cause name collisions with existing columns in the table,
            i.e., adding the suffix to a data item name
            should not produce the name of another data item in the same category.
            If a name collision occurs,
            an error will be raised during validation.
        dtype_float
            Polars data type to use for floating-point data items.
        dtype_int
            Polars data type to use for integer data items.
        cast_strict
            Whether to use strict casting for data type conversion.
            - If `True`, invalid values will raise errors during casting.
            - If `False`, invalid values will be converted to nulls/NaNs.
        bool_true
            Truthy strings for casting of "boolean"-type data items.
        bool_false
            Falsy strings for casting of "boolean"-type data items.
        bool_strip
            Whether to strip whitespace from "boolean"-type values before casting.
        bool_case_insensitive
            Whether to perform case-insensitive matching for "boolean"-type values.
        datetime_time_zone
            Time zone to use for datetime data items.
            If `None`, no time zone is applied.
        uchar_case_normalization
            Case normalization for "uchar"-type (case-insensitive) data values.
            If "lower", all values are converted to lowercase.
            If "upper", all values are converted to uppercase.
            If `None`, no case normalization is performed.
        enum_to_bool
            Whether to interpret enumerations with boolean-like values as booleans.
        enum_true
            List of strings representing `True` values for boolean enumerations.
        enum_false
            List of strings representing `False` values for boolean enumerations.
        add_category_info
            Whether to add category description, groups, and keys
            from the dictionary to each validated category.
        add_item_info
            Whether to add item description, mandatory flag, default value,
            enumeration, data type, range, and units
            from the dictionary to each validated data item.

        Returns
        -------
        validation_errors
            DataFrame of validation errors.
            Each row corresponds to a validation error,
            with the following columns:
            - "type": Type of validation error; one of:
              - "undefined_category": Category is not defined in the dictionary.
              - "undefined_item": Data item is not defined in the dictionary.
              - "missing_category": Mandatory category is missing.
              - "missing_item": Mandatory data item is missing.
              - "missing_value": Missing value ("?") for data item with no default.
              - "regex_violation": Value does not match data type regex.
              - "enum_violation": Value not in enumeration.
              - "range_violation": Value outside allowed range.
            - "block": Data block code where the error occurred, or `null` if not applicable.
            - "frame": Data frame code where the error occurred, or `null` if not applicable.
            - "category": Data category code where the error occurred, or `null` if not applicable.
            - "item": Data item name where the error occurred, or `null` if not applicable.
            - "column": Specific column name in the DataFrame where the error occurred,
              or `null` if not applicable.
              This should be the same as "item" unless the caster produces multiple columns for a data item
              (e.g., main column and ESD column), in which case this indicates the specific column.
            - "rows": List of (0-based) row indices in the category table with validation errors for the data item,
              or `null` if not applicable.

        Notes
        -----
        The validation proceeds as follows:
        1. The CIF file, data block, or data category is traversed.
        2. For each data block, missing mandatory categories are reported as "missing_category" errors.
        3. For each data category, if the category is not defined in the dictionary,
           an "undefined_category" error is reported.
           Otherwise, missing mandatory data items in the category
           are reported as "missing_item" errors.
           Additionally, if `add_category_info` is `True`,
           category metadata (description, groups, keys) from the dictionary
           are added to the category.
        4. For each data item (column) in the category,
           if the data item is not defined in the dictionary,
           an "undefined_item" error is reported. Otherwise:

           1. If the item has a default value defined,
              all missing ("?") values in the column are replaced with the default value.
              Otherwise, they are replaced with nulls,
              and the item (column) name and the row indices of missing values
              are reported as "missing_value" errors.
           2. All values in the column that are not `null` or "." (i.e., not missing or inapplicable)
              are checked against the construct regex.
              Column names and row indices of values that do not match the construct
              are reported as "regex_violation" errors.
           3. If the data item is of primitive type "uchar" and case normalization is specified,
              all values in the column are converted to the specified case.
           4. The data is converted to the appropriate data type:
              - "boolean": boolean type.
              - "date_dep", "yyyy-mm-dd", "yyyy-mm-dd:hh:mm", and "yyyy-mm-dd:hh:mm-flex":
                date or datetime type, depending on `datetime_output`.
              - "entity_id_list", "id_list", "id_list_spc", "symmetry_operation", and "ucode-alphanum-csv": List of strings.
              - "float": One floating-point column with the same name as the data item (containing values),
                plus an integer column with the specified `esd_col_suffix` suffix (containing estimated standard deviations).
              - "float-range": Two Array columns, each with 2 elements (min, max):
                - One floating-point Array column with the same name as the data item (containing value ranges).
                - One integer Array column with the specified `esd_col_suffix` suffix (containing estimated standard deviations for min and max).
              - "int": Integer type.
              - "int-range": Array column with 2 elements (min, max) of integers.
              - "int_list": List of integers.
              - "seq-one-letter-code" and "sequence_dep": String type with no whitespace.

              This also converts any inapplicable (".") values to nulls/NaNs/empty strings
              as appropriate for the data type
              (i.e., NaN for float, empty string for string, null for boolean/integer/date types).
           5. If the item has an enumeration defined,
              all values that are not missing or inapplicable (null/NaN/empty strings)
              are checked against the enumeration,
              and column names and row indices of values not in the enumeration are reported
              as "enum_violation" errors.
              If `enum_to_bool` is `True` and the values correspond to boolean-like values
              (i.e., all enumeration values are in `enum_true` or `enum_false`),
              the column is replaced with a boolean column.
              Otherwise, if all values are in the enumeration, the column is replaced
              with an Enum column (or List/Array of Enum, if applicable)
              with fixed categories defined by the enumeration
              (an extra category `""` for missing/inapplicable values is added automatically).
              Note that if the data item is of primitive type "uchar"
              and case normalization is specified,
              the enumeration values are also normalized to the specified case before checking/conversion.
           6. If the item has a range defined,
              all values are checked against the range,
              and column names and row indices of values outside the range are reported
              as "range_violation" errors.
              A range can only be defined for numeric data items.
           7. If `add_item_info` is `True`,
              item metadata (description, mandatory flag, default value,
              enumeration, data type, range, units) from the dictionary
              are added to the data item.
        """
        self._add_category_info = add_category_info
        self._add_item_info = add_item_info
        self._uchar_case_normalization = uchar_case_normalization
        self._enum_to_bool = enum_to_bool
        self._enum_true = {v.lower() for v in enum_true}
        self._enum_false = {v.lower() for v in enum_false}
        self._enum_bool = self._enum_true | self._enum_false
        self._caster = Caster(
            esd_col_suffix=esd_col_suffix,
            dtype_float=dtype_float,
            dtype_int=dtype_int,
            cast_strict=cast_strict,
            bool_true=bool_true,
            bool_false=bool_false,
            bool_strip=bool_strip,
            bool_case_insensitive=bool_case_insensitive,
            datetime_time_zone=datetime_time_zone,
        )
        self._errs = []

        if file.container_type == "category":
            self._validate_category(file)
            return pl.DataFrame(self._errs)

        blocks: list[CIFBlock] = [file] if file.container_type == "block" else file
        for block in blocks:
            self._curr_block_code = block.code
            for mandatory_cat in self._dict["mandatory_categories"]:
                self._curr_category_code = mandatory_cat
                if mandatory_cat not in block:
                    self._err("missing_category")
            for frame in block.frames:
                self._curr_frame_code = frame.code
                for frame_category in frame:
                    self._curr_category_code = frame_category.code
                    self._validate_category(frame_category)
            for block_category in block:
                self._curr_category_code = block_category.code
                self._validate_category(block_category)

        return pl.DataFrame(self._errs)

    def values_to_str(
        self,
        file: CIFFile | CIFBlock | CIFDataCategory,
        *,
        esd_col_suffix: str = "_esd_digits",
        bool_true: str = "YES",
        bool_false: str = "NO",
        enum_true: Sequence[str] = ("yes", "y", "true"),
        enum_false: Sequence[str] = ("no", "n", "false"),
        date_format: str = "%Y-%m-%d",
        datetime_format: str = "%Y-%m-%d:%H:%M",
        null_str: Literal[".", "?"] = "?",
        null_float: Literal[".", "?"] = "?",
        null_int: Literal[".", "?"] = "?",
        null_bool: Literal[".", "?"] = "?",
        empty_str: Literal[".", "?"] = ".",
        nan_float: Literal[".", "?"] = ".",
        drop_esd_columns: bool = True,
        uchar_case_normalization: Literal["lower", "upper"] | None = None,
    ) -> pl.DataFrame:
        """Convert typed DataFrame columns back to CIF string format.

        This method reverses the data type transformations performed by `validate()`,
        converting typed Polars columns back to their original CIF string representations.
        The conversion is done in-place, modifying the `df` property of each
        `CIFDataCategory` object.

        The method uses the DDL2 type code for each item to determine the correct
        reverse transformation, mirroring the type-based dispatch in `Caster`.

        Parameters
        ----------
        file
            CIF file, data block, or data category to convert back to strings.
            The method iterates over all contained `CIFDataCategory` objects
            (similar to `validate()`) and replaces each category's DataFrame
            with the stringified version.
        esd_col_suffix
            Suffix used for estimated standard deviation (ESD) columns.
            ESD columns will be merged back into the main column using parenthesized notation.
            For example, if a column "value" has an ESD column "value_esd_digits",
            the output will be "value(esd)" format like "1.234(5)".
        bool_true
            String to use for True values when converting "boolean"-type columns.
            Default is "YES" (standard CIF boolean true).
        bool_false
            String to use for False values when converting "boolean"-type columns.
            Default is "NO" (standard CIF boolean false).
        enum_true
            Sequence of strings (case-insensitive) representing truthy values
            for detecting boolean-like enum columns.
            Used to determine if an enumeration should be treated as boolean.
            Default is ("yes", "y", "true").
        enum_false
            Sequence of strings (case-insensitive) representing falsy values
            for detecting boolean-like enum columns.
            Default is ("no", "n", "false").
        date_format
            Format string for date output (strftime format).
            Default is "%Y-%m-%d" for "yyyy-mm-dd" format.
        datetime_format
            Format string for datetime output (strftime format).
            Default is "%Y-%m-%d:%H:%M" for "yyyy-mm-dd:hh:mm" format.
        null_str
            Symbol for null string values. Default is "?" (CIF unknown value).
        null_float
            Symbol for null float values. Default is "?" (CIF unknown value).
        null_int
            Symbol for null integer values. Default is "?" (CIF unknown value).
        null_bool
            Symbol for null boolean values. Default is "?" (CIF unknown value).
        empty_str
            Symbol for empty values (e.g., empty lists, NaN in int-range).
            Default is "." (CIF inapplicable value).
        nan_float
            Symbol for NaN float values. Default is "." (CIF inapplicable value).
        drop_esd_columns
            Whether to drop ESD columns from the output after merging.
            Default is True.
        uchar_case_normalization
            Case normalization for "uchar"-type (case-insensitive) string columns.
            If "lower", all string values are converted to lowercase.
            If "upper", all string values are converted to uppercase.
            If `None` (default), no case normalization is performed.

        Returns
        -------
        validation_errors
            DataFrame of validation errors.
            Each row corresponds to a validation error,
            with the following columns:
            - "type": Type of validation error; one of:
              - "undefined_item": Data item is not defined in the dictionary.
            - "block": Data block code where the error occurred, or `null` if not applicable.
            - "frame": Data frame code where the error occurred, or `null` if not applicable.
            - "category": Data category code where the error occurred, or `null` if not applicable.
            - "item": Data item name where the error occurred, or `null` if not applicable.

        Notes
        -----
        The reverse transformations are determined by DDL2 type code:

        - **"boolean"**: Uses `bool_true`/`bool_false` strings.
        - **Boolean-like enums**: Detects columns where dtype is Boolean and item
          has an enumeration with all values in `enum_true` or `enum_false`.
          Picks a consistent pair of values from the original enumeration
          (e.g., "yes"/"no" rather than "yes"/"n").
        - **"float"**: Merges with ESD column if present, NaN → `nan_float`.
        - **"float-range"**: Array → "min-max" or "val(esd)-val(esd)".
        - **"int"**: Cast to string, null → `null_int`.
        - **"int-range"**: Array → "min-max".
        - **"id_list", "entity_id_list", etc.**: List → comma-separated.
        - **"id_list_spc"**: List → space-separated.
        - **Date/datetime types**: Formatted per `date_format`/`datetime_format`.
        - **Enum columns**: Converted to plain strings, empty → `empty_str`.
        - **"uchar" primitive**: Case normalized per `uchar_case_normalization`.

        Examples
        --------
        >>> # After validate() produces typed columns
        >>> validator.validate(category)
        >>> # Convert back to strings for CIF output (in-place)
        >>> validator.values_to_str(category)
        """
        self._stringify_esd_col_suffix = esd_col_suffix
        self._stringify_enum_true_set = {v.lower() for v in enum_true}
        self._stringify_enum_false_set = {v.lower() for v in enum_false}
        self._stringify_enum_bool_set = self._stringify_enum_true_set | self._stringify_enum_false_set
        self._stringify_drop_esd_columns = drop_esd_columns
        self._stringify_uchar_case_normalization = uchar_case_normalization
        self._stringifier = Stringifier(
            esd_col_suffix=self._stringify_esd_col_suffix,
            bool_true=bool_true,
            bool_false=bool_false,
            date_format=date_format,
            datetime_format=datetime_format,
            null_str=null_str,
            null_float=null_float,
            null_int=null_int,
            null_bool=null_bool,
            empty_str=empty_str,
            nan_float=nan_float,
        )
        self._errs = []

        if file.container_type == "category":
            self._stringify_category(file)
            return pl.DataFrame(self._errs)

        blocks: list[CIFBlock] = [file] if file.container_type == "block" else file
        for block in blocks:
            self._curr_block_code = block.code
            for frame in block.frames:
                self._curr_frame_code = frame.code
                for frame_category in frame:
                    self._curr_category_code = frame_category.code
                    self._stringify_category(frame_category)
            for block_category in block:
                self._curr_category_code = block_category.code
                self._stringify_category(block_category)

        return pl.DataFrame(self._errs)

    def _stringify_category(self, cat: CIFDataCategory) -> None:
        """Convert a single category's DataFrame back to CIF string format."""
        df = cat.df
        category = cat.code

        # Get item definitions for this category
        item_defs = {}
        for data_item in cat:
            itemdef = self["item"].get(data_item.name)
            if itemdef is None and not data_item.name.endswith(self._stringify_esd_col_suffix):
                self._err("undefined_item", item=data_item.code)
            else:
                item_defs[data_item.code] = itemdef

        exprs: list[pl.Expr] = []
        columns_to_drop: set[str] = set()
        processed_cols: set[str] = set()

        for col_name in df.columns:
            if col_name in processed_cols:
                continue

            # Check if this is an ESD column - skip, will be handled with main column
            if col_name.endswith(self._stringify_esd_col_suffix):
                continue

            # Find item definition
            item_def = item_defs.get(col_name, {})
            esd_col_name = f"{col_name}{self._stringify_esd_col_suffix}"
            has_esd = esd_col_name in df.columns

            # Determine type code and primitive
            type_code = item_def.get("type", "any")
            # type_primitive is stored on the item definition after preprocessing
            type_prim = item_def.get("type_primitive", "char")

            # Get column dtype
            col_dtype = df.schema.get(col_name)

            # Check if this is a boolean-like enum:
            # - Column dtype is Boolean
            # - Item has an enumeration
            # - All enumeration values (case-insensitive) are in enum_true or enum_false
            bool_enum_true_val: str | None = None
            bool_enum_false_val: str | None = None

            if col_dtype == pl.Boolean and item_def:
                enum = item_def.get("enumeration", {})
                if enum:
                    enum_vals = list(enum.keys())
                    enum_vals_lower = {v.lower() for v in enum_vals}
                    if enum_vals_lower.issubset(self._stringify_enum_bool_set):
                        # Pick consistent pair from original enumeration
                        pair = pick_bool_enum_pair(
                            enum_vals,
                            self._stringify_enum_true_set,
                            self._stringify_enum_false_set,
                        )
                        if pair:
                            bool_enum_true_val, bool_enum_false_val = pair

            # Check if this column has an Enum dtype (non-bool enum)
            if isinstance(col_dtype, pl.Enum):
                # Use enum stringifier for Enum dtype columns
                plans = self._stringifier.enum(col_name)
            elif bool_enum_true_val is not None and bool_enum_false_val is not None:
                # Use bool_enum stringification with values from enumeration
                plans = self._stringifier(
                    col_name,
                    type_code,
                    has_esd=has_esd,
                    bool_enum_true=bool_enum_true_val,
                    bool_enum_false=bool_enum_false_val,
                )
            else:
                # Use type-code-based dispatch
                plans = self._stringifier(
                    col_name,
                    type_code,
                    has_esd=has_esd,
                )

            for plan in plans:
                expr = plan.expr
                # Apply uchar case normalization if applicable
                # The Stringifier outputs strings, so we can apply case normalization
                # unconditionally for uchar-primitive types
                if type_prim == "uchar" and self._stringify_uchar_case_normalization:
                    if self._stringify_uchar_case_normalization == "lower":
                        expr = expr.str.to_lowercase()
                    else:
                        expr = expr.str.to_uppercase()
                exprs.append(expr)
                processed_cols.update(plan.consumes)

            if has_esd and self._stringify_drop_esd_columns:
                columns_to_drop.add(esd_col_name)

        # Apply all string conversions
        result = df.with_columns(exprs)

        # Drop ESD columns if requested
        if columns_to_drop:
            result = result.drop(list(columns_to_drop))

        # Update the category's DataFrame in-place
        cat.df = result
        return

    def _validate_category(self, cat: CIFDataCategory) -> None:
        """Validate an mmCIF data category against the DDL2 dictionary."""
        catdef = self["category"].get(cat.code)
        if catdef is None:
            self._err(type="undefined_category")
        else:
            # Check existence of mandatory items in category
            for mandatory_item_name in catdef["mandatory_items"]:
                if mandatory_item_name not in cat.item_names:
                    self._err("missing_item", item=mandatory_item_name)
            # Add category info
            if self._add_category_info:
                cat.description = catdef["description"]
                cat.groups = catdef["groups"]
                cat.keys = catdef["keys"]

        item_defs = {}
        for data_item in cat:
            itemdef = self["item"].get(data_item.name)
            if itemdef is None:
                self._err("undefined_item", item=data_item.code)
            else:
                item_defs[data_item.code] = itemdef

        self._curr_item_defs = item_defs

        cat.df = self._validate_items(cat.df)

        # Add item info
        if self._add_item_info:
            for data_item in cat:
                itemdef = item_defs.get(data_item.code)
                if itemdef is None:
                    continue
                data_item.description = itemdef["description"]
                data_item.mandatory = itemdef["mandatory"]
                data_item.default = itemdef.get("default")
                data_item.enum = itemdef.get("enumeration")
                data_item.dtype = itemdef.get("type")
                data_item.range = itemdef.get("range")
                data_item.unit = itemdef.get("units")
        return

    def _validate_items(self, table: pl.DataFrame) -> pl.DataFrame:
        """Validate an mmCIF category table against category item definitions.

        Parameters
        ----------
        table
            mmCIF category table as a Polars DataFrame.
            Each column corresponds to a data item,
            and all values are strings or nulls.
            Strings represent parsed mmCIF values,
            i.e., with no surrounding quotes.

        Returns
        -------
        validated_table
            Processed mmCIF category table as a Polars DataFrame.
        """

        # Per spec: all values are strings or nulls.
        for name, dt in table.schema.items():
            if dt not in (pl.Utf8, pl.Null):
                raise TypeError(f"table column {name!r} must be Utf8 or Null; got {dt!r}")

        df = table.clone()

        # 1. Set defaults / collect missing values
        df = self._table_set_defaults(df)

        # 2. Validate regex patterns (ignore null and ".")
        self._table_check_regex(df)

        # 3. Case normalization for "uchar"
        if self._uchar_case_normalization:
            df = self._table_uchar_normalization(df)

        # 4. Cast data types
        df, produced_columns = self._table_cast(df)

        # 5. Apply enumerations
        df = self._table_enum(df, produced_columns=produced_columns)

        # 6. Range validation
        self._table_ranges(df, produced_columns=produced_columns)

        return df

    def _table_set_defaults(self, table: pl.DataFrame) -> pl.DataFrame:
        """Replace missing values ("?") with defaults in an mmCIF category table.

        For each item (column), if the item has a default value defined,
        all missing ("?") values in the column
        are replaced with the default value.
        Otherwise, the item (column) name and the row indices
        of missing values are collected,
        and missing values are replaced with nulls.

        Parameters
        ----------
        table
            mmCIF category table as a Polars DataFrame.
            Each column corresponds to a data item,
            and all values are strings or nulls.
            Strings represent parsed mmCIF values,
            i.e., with no surrounding quotes.
        item_defs
            Dictionary of data item definitions for the category.
            Keys are data item keywords (column names),
            and values are dictionaries with the following key-value pairs:
            - "default" (string | None): Default value for the data item (as a string),
            or `None` if no default is specified.
        block
            Current block code for error reporting.
        frame
            Current frame code for error reporting.
        category
            Current category name for error reporting.

        Returns
        -------
        updated_table
            Updated mmCIF category table as a Polars DataFrame,
            with missing values replaced as specified.
        missing_value_errors
            List of missing value error dictionaries.
        """
        # Build replacement expressions in one shot.
        replace_exprs: list[pl.Expr] = []
        miss_mask_exprs: list[pl.Expr] = []
        mask_col_prefix = "__missing_mask_col__"
        mask_cols: list[str] = []

        for item_name, item_def in self._curr_item_defs.items():
            col = pl.col(item_name)
            default = item_def.get("default")
            is_missing = col == pl.lit("?")
            replace_exprs.append(
                pl.when(is_missing).then(pl.lit(default)).otherwise(col).alias(item_name)
            )
            if default is None:
                # Track missing masks for error collection (only no-default items).
                mask_name = f"{mask_col_prefix}{item_name}"
                miss_mask_exprs.append(is_missing.alias(mask_name))
                mask_cols.append(mask_name)

        # Apply all replacements (and optionally mask cols) in one with_columns.
        # If no missing masks, return directly.
        if not miss_mask_exprs:
            return table.with_columns(replace_exprs)

        # Add masks temporarily for the single-pass error query.
        tmp = table.with_row_index("__row_idx").with_columns(replace_exprs + miss_mask_exprs)

        # Collect missing rows for all no-default items in one go.
        # Turn the boolean mask columns into long form:
        #   __row_idx | variable (__miss__col) | value (bool)
        long = (
            tmp.select(["__row_idx"] + mask_cols)
            .unpivot(index="__row_idx", variable_name="__miss_col", value_name="__is_missing")
            .filter(pl.col("__is_missing"))
            .with_columns(pl.col("__miss_col").str.strip_prefix(mask_col_prefix).alias("item"))
            .group_by("item")
            .agg(pl.col("__row_idx").cast(pl.Int64).alias("row_indices"))
        )

        miss_map = {r["item"]: r["row_indices"] for r in long.to_dicts()}

        for item_name, row_indices in miss_map.items():
            self._err(
                type="missing_value",
                item=item_name,
                column=item_name,
                rows=row_indices,
            )

        # Return table without temporary columns.
        updated = tmp.drop(["__row_idx"] + mask_cols)
        return updated

    def _table_check_regex(self, table: pl.DataFrame) -> None:
        """Check regex constraints on table columns."""
        for item_name, item_def in self._curr_item_defs.items():
            col = pl.col(item_name)
            type_regex = item_def["type_regex"]
            has_value = col.is_not_null() & (col != pl.lit("."))
            regex_violation = has_value & (~col.str.contains(f"^(?:{type_regex})$"))
            bad_rows = table.select(pl.arg_where(regex_violation)).to_series(0).to_list()
            if bad_rows:
                self._err(
                    type="regex_violation",
                    item=item_name,
                    column=item_name,
                    rows=bad_rows,
                )
        return None

    def _table_uchar_normalization(self, table: pl.DataFrame) -> pl.DataFrame:
        """Apply case normalization to "uchar" columns in an mmCIF category table.

        Parameters
        ----------
        table
            mmCIF category table as a Polars DataFrame.
            Each column corresponds to a data item,
            and all values are strings or nulls.
            Strings represent parsed mmCIF values,
            i.e., with no surrounding quotes.
        item_defs
            Dictionary of data item definitions for the category.
            Keys are data item keywords (column names),
            and values are dictionaries with the following key-value pairs:
            - "type_primitive" (string): Primitive type of the data item.
            One of: "char", "uchar", "numb".
        case_normalization
            Case normalization for "uchar" (case-insensitive character) data items.
            If "lower", all values are converted to lowercase.
            If "upper", all values are converted to uppercase.

        Returns
        -------
        updated_table
            Updated mmCIF category table as a Polars DataFrame,
            with case normalization applied to "uchar" columns.
        """
        transforms: list[pl.Expr] = []
        for item_name, item_def in self._curr_item_defs.items():
            if item_def["type_primitive"] != "uchar":
                continue
            col = pl.col(item_name)
            transform = (
                col.str.to_lowercase() if self._uchar_case_normalization == "lower" else col.str.to_uppercase()
            ).alias(item_name)
            transforms.append(transform)

        return table.with_columns(transforms)

    def _table_cast(self, table: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, list[_ProducedColumn]]]:
        outs_seen: set[str] = set()
        exprs: list[pl.Expr] = []
        produced_entries: dict[str, list[_ProducedColumn]] = {}
        for item_name, item_def in self._curr_item_defs.items():
            type_code = item_def["type"]
            col = pl.col(item_name)
            plans = self._caster(col, type_code)
            produced = produced_entries[item_name] = []
            for plan in plans:
                output_col_name = f"{item_name}{plan.suffix}"
                if output_col_name in outs_seen:
                    raise ValueError(f"caster produced duplicate output name {output_col_name!r} for item {item_name!r}")
                outs_seen.add(output_col_name)
                exprs.append(plan.expr.alias(output_col_name))
                produced.append(
                    _ProducedColumn(
                        input_name=item_name,
                        output_name=output_col_name,
                        plan=plan,
                        type_code=type_code,
                    )
                )

        df = table.with_columns(exprs)
        return df, produced_entries

    def _table_enum(self, table: pl.DataFrame, produced_columns: dict[str, list[_ProducedColumn]]) -> pl.DataFrame:
        exprs: list[pl.Expr] = []

        for item_name, item_def in self._curr_item_defs.items():
            enum = list(item_def.get("enumeration", {}).keys())
            if not enum:
                continue

            type_prim = item_def["type_primitive"]
            # Normalize enum values (per item) if needed.
            enum_vals_norm: list[str] = (
                enum
                if type_prim != "uchar" or not self._uchar_case_normalization
                else _normalize_vals(enum, self._uchar_case_normalization)
            )

            enum_vals_lower = {v.lower() for v in enum_vals_norm}
            bool_like: bool = self._enum_to_bool and enum_vals_lower.issubset(self._enum_bool)

            for produced_column in produced_columns[item_name]:
                plan = produced_column.plan

                # Only string or int allowed
                if plan.dtype not in ("str", "int"):
                    raise TypeError(
                        f"Enum specified for item {item_name!r}, but main produced column {produced_column.output_name!r} "
                        f"has leaf dtype {plan.dtype!r}"
                    )

                # Skip auxiliary columns
                if not produced_column.plan.main:
                    continue

                tmp_col = pl.col(produced_column.output_name)

                def pred(el: pl.Expr) -> pl.Expr:
                    n = _leaf_nullish_for_validation(el, plan)
                    return (~n) & (~el.cast(pl.Utf8).is_in(enum_vals_norm))

                viol = _any_violation(tmp_col, plan, pred)
                viol_rows = _collect_rows(table, viol)
                if viol_rows:
                    self._err(
                        type="enum_violation",
                        item=item_name,
                        column=produced_column.output_name,
                        rows=viol_rows,
                    )
                    continue

                if bool_like:
                    # Convert leaves to boolean (case-insensitive).
                    def mapper(el: pl.Expr) -> pl.Expr:
                        ci = el.cast(str).str.to_lowercase()
                        return (
                            pl
                            .when(ci.is_in(list(self._enum_true))).then(pl.lit(True))
                            .when(ci.is_in(list(self._enum_false))).then(pl.lit(False))
                            .otherwise(pl.lit(None))
                        )
                else:
                    enum_dtype = pl.Enum(enum_vals_norm + [""])
                    # Convert leaves to Enum while preserving nullish leaves.
                    def mapper(el: pl.Expr) -> pl.Expr:
                        return el.cast(str).cast(enum_dtype)

                new_col = _map_leaves(tmp_col, plan, mapper).alias(produced_column.output_name)
                exprs.append(new_col)

        df = table.with_columns(exprs) if exprs else table
        return df

    def _table_ranges(self, table: pl.DataFrame, produced_columns: dict[str, list[_ProducedColumn]]) -> None:

        for item_name, item_def in self._curr_item_defs.items():
            ranges = item_def.get("range")
            if ranges is None:
                continue

            type_prim = item_def["type_primitive"]

            if type_prim != "numb":
                raise TypeError(
                    f"Range specified for non-numeric item {item_name!r} (type_primitive={type_prim!r})"
                )

            for produced_column in produced_columns[item_name]:
                if not produced_column.plan.main:
                    continue

                plan = produced_column.plan
                if plan.dtype not in ("float", "int"):
                    raise TypeError(
                        f"Range specified for item {item_name!r}, but produced column {produced_column.output_name!r} "
                        f"has leaf dtype {plan.dtype!r}"
                    )

                tmp_col = pl.col(produced_column.output_name)

                def pred(el: pl.Expr) -> pl.Expr:
                    n = _leaf_nullish_for_validation(el, plan)
                    return (~n) & (~_allowed_by_ranges(el, ranges))

                viol = _any_violation(tmp_col, plan, pred)
                viol_rows = _collect_rows(table, viol)
                if viol_rows:
                    self._err(
                        type="range_violation",
                        item=item_name,
                        column=produced_column.output_name,
                        rows=viol_rows,
                    )
        return

    def _err(
        self,
        type: Literal[
            "undefined_category",
            "undefined_item",
            "missing_category",
            "missing_item",
            "missing_value",
            "regex_violation",
            "enum_violation",
            "range_violation",
        ],
        *,
        item: str | None = None,
        column: str | None = None,
        rows: list[int] | None = None,
    ) -> None:
        """Create an error dictionary."""
        err = {
            "type": type,
            "block": self._curr_block_code,
            "frame": self._curr_frame_code,
            "category": self._curr_category_code,
            "item": item,
            "column": column,
            "rows": rows,
        }
        self._errs.append(err)
        return


def _normalize_for_rust_regex(regex: str) -> str:
    """Normalize a regex for use in Rust-based validation.

    This function applies necessary transformations to ensure compatibility
    with the Rust regex engine used in certain validation contexts.

    Parameters
    ----------
    regex
        The input regex string to be normalized.

    Returns
    -------
    str
        The normalized regex string.
    """
    # DDL2 regexes contain unescaped square brackets inside character classes,
    # which are not supported by the Rust regex engine.
    # Escape them here.
    regex = regex.replace(r"[][", r"[\]\[")
    return regex


@dataclass(frozen=True)
class _ProducedColumn:
    """One produced column emitted by one caster for one input item."""
    input_name: str
    output_name: str
    plan: CastPlan
    type_code: str


def _collect_rows(df: pl.DataFrame, mask: pl.Expr) -> list[int]:
    # Eager: returns row indices where mask is True.
    return df.select(pl.arg_where(mask)).to_series(0).to_list()


def _normalize_vals(
    vals: Sequence[str],
    mode: Literal["lower", "upper"]
) -> list[str]:
    return [v.lower() for v in vals] if mode == "lower" else [v.upper() for v in vals]


def _leaf_nullish_for_validation(el: pl.Expr, plan: Any) -> pl.Expr:
    """
    Nullish markers (to be ignored) for enum/range validation, at the LEAF level.

    Per spec:
    - float: null or NaN
    - str: null or empty string
    - int/bool/date: null
    """
    if plan.dtype == "float":
        return el.is_null() | el.is_nan()
    if plan.dtype == "str":
        return el.is_null() | (el == pl.lit(""))
    return el.is_null()


def _any_violation(
    col: pl.Expr,
    plan: Any,
    pred_leaf: Callable[[pl.Expr], pl.Expr]
) -> pl.Expr:
    """
    Per-row boolean: True if ANY innermost leaf element violates pred_leaf.
    Container semantics (as agreed):
    - None: scalar
    - list: validate elements
    - array: validate all array elements
    - array_list: validate all elements in each array in the list
    """
    if plan.container is None:
        return pred_leaf(col)
    if plan.container == "list":
        return col.list.eval(pred_leaf(pl.element())).list.any()
    if plan.container == "array":
        return col.arr.eval(pred_leaf(pl.element())).arr.any()
    if plan.container == "array_list":
        return col.list.eval(
            pl.element().arr.eval(pred_leaf(pl.element())).arr.any()
        ).list.any()
    raise ValueError(f"Unsupported container: {plan.container!r}")


def _map_leaves(
    col: pl.Expr,
    plan: Any,
    mapper: Callable[[pl.Expr], pl.Expr]
) -> pl.Expr:
    """
    Apply `mapper` to each innermost leaf element, preserving container structure.
    """
    if plan.container is None:
        return mapper(col)
    if plan.container == "list":
        return col.list.eval(mapper(pl.element()))
    if plan.container == "array":
        return col.arr.eval(mapper(pl.element()))
    if plan.container == "array_list":
        return col.list.eval(pl.element().arr.eval(mapper(pl.element())))
    raise ValueError(f"Unsupported container: {plan.container!r}")


def _allowed_by_ranges(
    el: pl.Expr,
    ranges: list[tuple[float | None, float | None]]
) -> pl.Expr:
    """
    Leaf predicate: True if `el` lies in the union of the specified ranges.
    Ranges are exclusive bounds, except lo==hi means exact match.
    """
    allowed: pl.Expr | None = None
    for lo, hi in ranges:
        if lo is None and hi is None:
            ok = pl.lit(True)
        elif lo is not None and hi is not None and lo == hi:
            ok = el == pl.lit(lo)
        else:
            ok = pl.lit(True)
            if lo is not None:
                ok = ok & (el > pl.lit(lo))
            if hi is not None:
                ok = ok & (el < pl.lit(hi))
        allowed = ok if allowed is None else (allowed | ok)
    return allowed if allowed is not None else pl.lit(True)
