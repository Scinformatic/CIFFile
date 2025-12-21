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
    def _normalize_case_expr(expr: pl.Expr, mode: Literal["lower", "upper"]) -> pl.Expr:
        if mode == "lower":
            return expr.str.to_lowercase()
        return expr.str.to_uppercase()

    def _normalize_case_values(values: list[str], mode: Literal["lower", "upper"]) -> list[str]:
        if mode == "lower":
            return [v.lower() for v in values]
        return [v.upper() for v in values]

    def _is_nullish_scalar(expr: pl.Expr) -> pl.Expr:
        # "nullish" here means: null OR NaN OR empty string
        # (covers typical aux columns and scalar data after casting).
        return (
            expr.is_null()
            | pl.when(expr.is_nan().is_not_null()).then(expr.is_nan()).otherwise(False)
            | pl.when(expr.dtype == pl.Utf8).then(expr == "").otherwise(False)  # best-effort
        )

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
        if item not in df.columns:
            continue

        default: str | None = idef.get("default")
        enum: list[str] | None = idef.get("enum")
        ranges: list[tuple[float | None, float | None]] | None = idef.get("range")
        caster = idef.get("caster")
        type_primitive: str = idef.get("type_primitive")
        type_regex: str = idef.get("type_regex")

        if not callable(caster):
            raise TypeError(f'item_defs["{item}"]["caster"] must be callable')

        # --- Step 1: handle "?" missing values (and collect missing_value errors if no default).
        missing_mask = pl.col(item).cast(pl.Utf8) == pl.lit("?")
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
                pl.when(missing_mask).then(pl.lit(None)).otherwise(pl.col(item)).alias(item)
            )

        # --- Step 2: construct regex check (exclude null and "." only).
        # Treat the table as strings here; mmCIF raw tables commonly are strings.
        non_null_non_dot = pl.col(item).is_not_null() & (pl.col(item).cast(pl.Utf8) != pl.lit("."))
        construct_ok = _fullmatch_regex_expr(pl.col(item).cast(pl.Utf8), type_regex)
        construct_mismatch_mask = non_null_non_dot & (~construct_ok)
        construct_bad_rows = _collect_row_indices(df, construct_mismatch_mask)
        if construct_bad_rows:
            errors.append(
                {
                    "item": item,
                    "row_indices": construct_bad_rows,
                    "error_type": "construct_mismatch",
                }
            )

        # --- Step 3: normalize case for uchar (applied on non-null and not ".")
        if type_primitive == "uchar" and case_normalization in ("lower", "upper"):
            norm = case_normalization
            df = df.with_columns(
                pl.when(pl.col(item).is_null() | (pl.col(item).cast(pl.Utf8) == pl.lit(".")))
                .then(pl.col(item))
                .otherwise(_normalize_case_expr(pl.col(item).cast(pl.Utf8), norm))
                .alias(item)
            )

        # --- Step 4: cast via caster; add/merge auxiliary columns
        exprs = caster(pl.col(item))
        if not isinstance(exprs, list) or not exprs:
            raise TypeError(f'caster for "{item}" must return a non-empty list of Polars expressions')

        main_expr = exprs[0]
        # Ensure the main expression aliases back to the item name
        if getattr(main_expr, "meta", None) is not None:
            try:
                out_name = main_expr.meta.output_name()
            except Exception:
                out_name = None
        else:
            out_name = None
        if out_name != item:
            main_expr = main_expr.alias(item)

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

