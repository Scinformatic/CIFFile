"""DDL2 validator."""

from typing import Any, Sequence, Literal, Callable
from functools import partial

import polars as pl

from ciffile.structure import CIFFile, CIFBlock, CIFDataCategory
from ._validator import CIFFileValidator
from ._ddl2_model import DDL2Dictionary
from ._ddl2_types import Caster, CastPlan
from ._re import normalize_for_rust_regex


class DDL2Validator(CIFFileValidator):
    """DDL2 validator for CIF files."""

    def __init__(
        self,
        dictionary: dict,
        *,
        enum_to_bool: bool = True,
        enum_true: Sequence[str] = ("yes", "y", "true"),
        enum_false: Sequence[str] = ("no", "n", "false"),
        esd_col_suffix: str = "_esd",
        dtype_float: pl.DataType = pl.Float64,
        dtype_int: pl.DataType = pl.Int64,
        cast_strict: bool = True,
        bool_true: Sequence[str] = ("YES",),
        bool_false: Sequence[str] = ("NO",),
        bool_strip: bool = True,
        bool_case_insensitive: bool = True,
        datetime_output: Literal["auto", "date", "datetime"] = "auto",
        datetime_time_zone: str | None = None,
    ) -> None:
        super().__init__(dictionary)
        DDL2Dictionary(**dictionary)  # validate dictionary structure
        self._caster = Caster(
            esd_col_suffix=esd_col_suffix,
            dtype_float=dtype_float,
            dtype_int=dtype_int,
            cast_strict=cast_strict,
            bool_true=bool_true,
            bool_false=bool_false,
            bool_strip=bool_strip,
            bool_case_insensitive=bool_case_insensitive,
            datetime_output=datetime_output,
            datetime_time_zone=datetime_time_zone,
        )

        dictionary["mandatory_categories"] = mandatory_categories = []
        for category_id, category in dictionary["category"].items():

            category["mandatory_items"] = []

            if category["mandatory"]:
                mandatory_categories.append(category_id)

            category["groups"] = {
                group_id: dictionary["category_group"][group_id]
                for group_id in category.get("groups", [])
            }

        self._bool_true = set(enum_true)
        self._bool_false = set(enum_false)

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

            if enum_to_bool and "enumeration" in item and all(enum in bool_enums for enum in item["enumeration"].keys()):
                item_type = item["type"] = "boolean"
                item.pop("enumeration")

            item_type_info = dictionary["item_type"][item_type]
            item["type_primitive"] = item_type_info["primitive"]
            item["type_regex"] = normalize_for_rust_regex(item_type_info["regex"]).pattern
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


        item_defs = {}
        for data_item in cat:
            try:
                itemdef = self["item"][data_item.name]
            except KeyError:
                raise ValueError(
                    f"DDL2Validator: Data item '{data_item.name}' "
                    f"not defined in DDL2 dictionary."
                )
            item_defs[data_item.code] = itemdef

        new_df, item_errs = validate_mmcif_category_table(
            cat.df,
            item_defs,
            case_normalization="lower",
        )
        errs.extend(item_errs)
        cat.df = new_df

        # Add item info
        if add_item_info:
            for data_item in cat:
                itemdef = item_defs[data_item.code]
                data_item.description = itemdef["description"]
                data_item.mandatory = itemdef["mandatory"]
                data_item.default = itemdef.get("default")
                data_item.enum = itemdef.get("enumeration")
                data_item.dtype = itemdef.get("type")
                data_item.range = itemdef.get("range")
                data_item.unit = itemdef.get("units")


        return errs


def validate_mmcif_category_table(
    table: pl.DataFrame,
    item_defs: dict[str, dict[str, Any]],
    caster: Callable[[str | pl.Expr, str], list[CastPlan]],
    case_normalization: Literal["lower", "upper"] | None = "lower",
    enum_to_bool: bool = True,
    enum_true: Sequence[str] = ("yes", "y", "true"),
    enum_false: Sequence[str] = ("no", "n", "false"),
) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    """Validate an mmCIF category table against category item definitions.

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
        - "enum" (list of strings | None): List of allowed values for the data item;
          or `None` if no enumeration is specified.
        - "range" (list of 2-tuples of floats or None | None): List of allowed ranges for the data item.
          Each range is a 2-tuple indicating an exclusive minimum and maximum value, respectively.
          A value of `None` for minimum or maximum indicates no bound in that direction.
          If both minimum and maximum are the same non-None float value,
          it indicates that only that exact value is allowed.
          The allowed range for the data item is the union of all specified ranges.
          If `None`, no range is specified.
        - "type" (string): Data item type code, corresponding to a DDL2 item type defined.
        - "type_primitive" ({"numb", "char", "uchar"}): Primitive data type code; one of:
          - "numb": numerically intererpretable string
          - "char": case-sensitive character or text string
          - "uchar": case-insensitive character or text string
        - "type_regex" (string): Data type construct (regex).
    caster
        Data type casting function for the data item.
        The function takes a column name or Polars expression as first input,
        and the data type code (`item_defs[item]["type"]`) as second input,
        and returns a list of one or several `CastPlan` objects with the following attributes:
        - `expr` (pl.Expr): Polars expression that yields a column from the input column.
        - `dtype` (literal): Type of the leaf data values produced by the expression; one of:
            - "str": string
            - "float": floating-point number
            - "int": integer
            - "bool": boolean
            - "date": date/datetime
        - `container` (literal or None): Container type of the data values; one of:
            - None: No container; scalar values
            - "list": List of values
            - "array": Array of values
            - "array_list": List of arrays of values

            Together with `dtype`, this indicates the structure of the data values.
            For example, if `dtype` is "float" and `container` is "array_list",
            it indicates that each element in the output column
            is a List of Arrays of floating-point numbers.
        - `suffix` (string): Suffix to add to the input column name for the output column.
          If empty string, the output column has the same name as the input column.
        - `main` (boolean): Whether the column contains main data values,
          i.e., values for which other validations (enumeration, range) are performed.
          If `False`, the column contains auxiliary data values
          (e.g., estimated standard deviations) that are not subject to these validations.
          Note that more than one main column may be produced by the caster function.

        The input column is thus replaced with the set of columns produced by the caster function.
        Note that the suffix may cause name collisions with existing columns in the table.
        These are handled as described below.
    case_normalization
        Case normalization for "uchar" (case-insensitive character) data items.
        If "lower", all values are converted to lowercase.
        If "upper", all values are converted to uppercase.
        If `None`, no case normalization is performed.
    enum_to_bool
        Whether to interpret enumerations with boolean-like values as booleans.
    enum_true
        List of strings representing `True` values for boolean enumerations.
    enum_false
        List of strings representing `False` values for boolean enumerations.

    Returns
    -------
    validated_table
        Processed mmCIF category table as a Polars DataFrame.
    validation_errors
        List of validation error dictionaries.
        Each dictionary contains the following key-value pairs:
        - "item" (string): Data item (column) name.
        - "column" (string): Specific column name in the DataFrame where the error occurred.
          When the caster produces multiple columns for a data item, this indicates the specific column.
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
       as appropriate for the data type
       (i.e., NaN for float, empty string for string, null for boolean/integer/date types).
    5. If the item has an enumeration defined,
       all values in the "main" produced columns that are not null/NaN/empty strings
       are checked against the enumeration,
       and column names and row indices of values not in the enumeration are collected.
       If all values are in the enumeration, the column is replaced
       with an Enum column (or List/Array of Enum, if applicable) with fixed categories defined by the enumeration.
       If `enum_to_bool` is `True` and the  values corresponds to boolean-like values
       (i.e., all enumeration values are in `enum_true` or `enum_false`; case-insensitive),
       the column is replaced with a boolean column.
       Note that if the data item is of primitive type "uchar"
       and case normalization is specified,
       the enumeration values are also normalized to the specified case before checking/conversion.
    6. If the item has a range defined,
       all values in the "main" produced columns are checked against the range,
       and column names and row indices of values outside the range are collected.
       A range is only defined for numeric data items.
    7. The input column is replaced with the casted and transformed column(s).
       It may be the case that the caster function produces columns with names
       that already exist in the input table (due to suffixes; e.g.,
       an input column "coord" may need to be replaced with "coord" and "coord_esd",
       while "coord_esd" may already exist in the input table).
       In this case, for each such column:
         - For rows where the casted original column value is null/NaN,
           the value from the caster-produced column is used.
         - For rows where the casted original column value is not null/NaN,
           it is compared with the caster-produced column,
           and any discrepancies are collected.

        Note that this step is performed after all columns have been processed,
        since otherwise we may be comparing one casted column against another non-casted raw column.
    """
    def _normalize_case_expr(expr: pl.Expr, mode: Literal["lower", "upper"]) -> pl.Expr:
        if mode == "lower":
            return expr.str.to_lowercase()
        return expr.str.to_uppercase()

    def _normalize_case_values(values: list[str], mode: Literal["lower", "upper"]) -> list[str]:
        if mode == "lower":
            return [v.lower() for v in values]
        return [v.upper() for v in values]

    def _safe_is_nan(expr: pl.Expr) -> pl.Expr:
        # Polars raises for is_nan on non-floats; guard it.
        return pl.when(expr.is_nan().is_not_null()).then(expr.is_nan()).otherwise(False)

    def _collect_row_indices(df: pl.DataFrame, mask: pl.Expr) -> list[int]:
        # mask is a boolean per-row expression
        return df.select(pl.arg_where(mask)).to_series(0).to_list()

    def _fullmatch_regex_expr(expr: pl.Expr, regex: str) -> pl.Expr:
        # Force a full match even if regex is not anchored.
        pat = f"^(?:{regex})$"
        return expr.cast(pl.Utf8).str.contains(pat)

    def _innermost_violation_any(
        col_expr: pl.Expr,
        predicate_on_element: Callable[[pl.Expr], pl.Expr],
        max_depth: int = 8,
    ) -> pl.Expr:
        """
        Return a per-row bool: True if any innermost element violates predicate.

        Handles scalar and nested List columns (List, List[List], ...).
        For non-List complex types, it falls back to scalar treatment.
        """
        # Try nested list evaluation a few times; if at runtime it's not a list, Polars will error.
        # To avoid hard failures, we only use list.eval while dtype stays List in eager context
        # by switching to a runtime-friendly expression: we attempt list.eval and rely on schema
        # at the moment we build expressions (post-cast).
        e = col_expr
        depth = 0
        while depth < max_depth:
            # We can inspect dtype only if it's a literal column expr? In practice, we call this
            # with pl.col(name) on an eager DataFrame with known schema (post-cast), so this works.
            # If not, we bail out early.
            try:
                # This attribute access is supported on Expr in modern Polars; if absent, except.
                dtype = e.dtype  # type: ignore[attr-defined]
            except Exception:
                dtype = None

            if dtype is not None and isinstance(dtype, pl.List):
                # Evaluate one level down.
                e = e.list.eval(pl.element())
                depth += 1
                continue
            break

        # If still list-typed, apply predicate at element level and reduce with list.any().
        try:
            dtype = e.dtype  # type: ignore[attr-defined]
        except Exception:
            dtype = None

        if dtype is not None and isinstance(dtype, pl.List):
            # Apply predicate to (one-level) list elements.
            viol_list = e.list.eval(predicate_on_element(pl.element()))
            return viol_list.list.any()
        # Scalar
        return predicate_on_element(e)

    def _enum_violation_mask(col_expr: pl.Expr, enum_values: list[str]) -> pl.Expr:
        def pred(el: pl.Expr) -> pl.Expr:
            # Ignore null/NaN/empty
            return (
                (~el.is_null())
                & (~_safe_is_nan(el))
                & pl.when(el.dtype == pl.Utf8).then(el != "").otherwise(True)  # best-effort
                & (~el.is_in(enum_values))
            )

        return _innermost_violation_any(col_expr, pred)

    def _range_allowed_predicate(el: pl.Expr, ranges: list[tuple[float | None, float | None]]) -> pl.Expr:
        allowed_any = None
        for lo, hi in ranges:
            if lo is None and hi is None:
                this_allowed = pl.lit(True)
            elif lo is not None and hi is not None and lo == hi:
                this_allowed = el == pl.lit(lo)
            else:
                parts: list[pl.Expr] = []
                if lo is not None:
                    parts.append(el > pl.lit(lo))
                if hi is not None:
                    parts.append(el < pl.lit(hi))
                this_allowed = parts[0]
                for p in parts[1:]:
                    this_allowed = this_allowed & p
            allowed_any = this_allowed if allowed_any is None else (allowed_any | this_allowed)
        return allowed_any if allowed_any is not None else pl.lit(True)

    def _range_violation_mask(
        col_expr: pl.Expr,
        ranges: list[tuple[float | None, float | None]],
    ) -> pl.Expr:
        def pred(el: pl.Expr) -> pl.Expr:
            # Ignore null/NaN
            allowed = _range_allowed_predicate(el, ranges)
            return (~el.is_null()) & (~_safe_is_nan(el)) & (~allowed)

        return _innermost_violation_any(col_expr, pred)

    if not isinstance(table, pl.DataFrame):
        raise TypeError("table must be a polars.DataFrame")

    df = table.clone()
    errors: list[dict[str, Any]] = []

    # Only validate items that are present in item_defs and exist in the table.
    # (No implicit creation of missing columns here.)
    for item, idef in item_defs.items():

        default: str | None = idef.get("default")
        enum: list[str] | None = idef.get("enum")
        ranges: list[tuple[float | None, float | None]] | None = idef.get("range")
        caster = idef["caster"]
        type_primitive: str = idef["type_primitive"]
        type_regex: str = idef["type_regex"]

        # --- Step 1: handle "?" missing values (and collect missing_value errors if no default).
        missing_mask = pl.col(item) == pl.lit("?")
        if default is not None:
            df = df.with_columns(
                pl.when(missing_mask).then(pl.lit(default)).otherwise(pl.col(item)).alias(item)
            )
        else:
            missing_rows = _collect_row_indices(df, missing_mask)
            if missing_rows:
                errors.append(
                    {"item": item, "row_indices": missing_rows, "error_type": "missing_value"}
                )
            df = df.with_columns(
                pl.when(missing_mask).then(pl.lit(None)).otherwise(pl.col(item))
            )

        # --- Step 2: construct regex check (exclude null and "." only).
        # Treat the table as strings here; mmCIF raw tables commonly are strings.
        # non_null_non_dot = pl.col(item).is_not_null() & (pl.col(item).cast(pl.Utf8) != pl.lit("."))
        # construct_ok = _fullmatch_regex_expr(pl.col(item).cast(pl.Utf8), type_regex)
        # construct_mismatch_mask = non_null_non_dot & (~construct_ok)
        # construct_bad_rows = _collect_row_indices(df, construct_mismatch_mask)
        # if construct_bad_rows:
        #     errors.append(
        #         {
        #             "item": item,
        #             "row_indices": construct_bad_rows,
        #             "error_type": "construct_mismatch",
        #         }
        #     )

        # --- Step 3: normalize case for uchar (applied on non-null and not ".")
        if type_primitive == "uchar" and case_normalization in ("lower", "upper"):
            df = df.with_columns(_normalize_case_expr(pl.col(item), case_normalization))

        # --- Step 4: cast via caster; add/merge auxiliary columns
        exprs = caster(pl.col(item))
        if isinstance(exprs, pl.Expr):
            exprs = [exprs]

        main_expr = exprs[0]


        produced_aux_exprs: list[pl.Expr] = []
        produced_aux_names: list[str] = []
        for aux_expr in exprs[1:]:
            if getattr(aux_expr, "meta", None) is None:
                raise ValueError(f'auxiliary expression for "{item}" lacks metadata/alias')
            aux_name = aux_expr.meta.output_name()
            if not aux_name:
                raise ValueError(f'auxiliary expression for "{item}" must have an alias')
            produced_aux_exprs.append(aux_expr)
            produced_aux_names.append(aux_name)

        # Compute casted + produced aux into a temporary DF to compare/merge reliably.
        tmp_cols = [main_expr] + [e.alias(n) for e, n in zip(produced_aux_exprs, produced_aux_names)]
        tmp = df.select(pl.all(), *tmp_cols)

        # Replace df's main column with casted version.
        df = df.with_columns(tmp[item])

        # Merge auxiliary columns according to spec.
        for aux_name in produced_aux_names:
            produced = tmp[aux_name]
            if aux_name not in df.columns:
                df = df.with_columns(pl.lit(produced).alias(aux_name))
                continue

            # Compare non-nullish input vs produced; fill nullish with produced.
            input_col = pl.col(aux_name)
            produced_col = pl.lit(produced).alias(aux_name)

            # Nullish detection for aux columns: null OR NaN (and treat empty string as nullish too).
            input_nullish = input_col.is_null() | _safe_is_nan(input_col) | (
                input_col.cast(pl.Utf8, strict=False) == pl.lit("")
            )
            produced_nullish = produced_col.is_null() | _safe_is_nan(produced_col) | (
                produced_col.cast(pl.Utf8, strict=False) == pl.lit("")
            )

            # Equality with NaN handling (NaN == NaN should be treated as equal here).
            both_nan = _safe_is_nan(input_col) & _safe_is_nan(produced_col)
            equal_or_both_nan = (input_col == produced_col) | both_nan

            mismatch_mask = (
                (~input_nullish)
                & (~produced_nullish)
                & (~equal_or_both_nan)
            )

            bad_rows = _collect_row_indices(df, mismatch_mask)
            if bad_rows:
                errors.append(
                    {"item": aux_name, "row_indices": bad_rows, "error_type": "auxiliary_mismatch"}
                )

            df = df.with_columns(
                pl.when(input_nullish).then(produced_col).otherwise(input_col).alias(aux_name)
            )

        # --- Step 5: enum check + categorical cast (only if no enum violations).
        if enum is not None:
            enum_values = list(enum)
            if type_primitive == "uchar" and case_normalization in ("lower", "upper"):
                enum_values = _normalize_case_values(enum_values, case_normalization)

            enum_bad_rows = _collect_row_indices(df, _enum_violation_mask(pl.col(item), enum_values))
            if enum_bad_rows:
                errors.append(
                    {"item": item, "row_indices": enum_bad_rows, "error_type": "enum_violation"}
                )
            else:
                # Best-effort categorical; Polars does not reliably let us pin category order here.
                df = df.with_columns(pl.col(item).cast(pl.Categorical).alias(item))

        # --- Step 6: range check (numeric-ish columns; applied to innermost values).
        if ranges is not None:
            range_bad_rows = _collect_row_indices(df, _range_violation_mask(pl.col(item), ranges))
            if range_bad_rows:
                errors.append(
                    {"item": item, "row_indices": range_bad_rows, "error_type": "range_violation"}
                )

    return df, errors

