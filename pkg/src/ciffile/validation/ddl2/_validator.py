"""DDL2 validator."""

from __future__ import annotations

from typing import Any, Sequence, Literal, Callable, TYPE_CHECKING
from dataclasses import dataclass

import polars as pl


from .._base import CIFFileValidator
from ._input_schema import DDL2Dictionary
from ._caster import Caster, CastPlan

if TYPE_CHECKING:
    from ciffile.structure import CIFFile, CIFBlock, CIFDataCategory


class DDL2Validator(CIFFileValidator):
    """DDL2 validator for CIF files."""

    def __init__(
        self,
        dictionary: dict,
        *,
        enum_to_bool: bool = True,
        enum_true: Sequence[str] = ("yes", "y", "true"),
        enum_false: Sequence[str] = ("no", "n", "false"),
        esd_col_suffix: str = "_esd_digits",
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

            item_type_info = dictionary["item_type"][item_type]
            item["type_primitive"] = item_type_info["primitive"]
            item["type_regex"] = _normalize_for_rust_regex(item_type_info["regex"])
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
    ) -> pl.DataFrame:
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

        if file.container_type == "category":
            return pl.DataFrame(validate_category(file))

        blocks = [file] if file.container_type == "block" else file
        errs = []
        for block in blocks:
            for mandatory_cat in self._dict["mandatory_categories"]:
                if mandatory_cat not in block:
                    errs.append(
                        self._err(
                            type="missing_category",
                            block=block.code,
                            category=mandatory_cat,
                        )
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
        return pl.DataFrame(errs)

    def _validate_category(
        self,
        cat: CIFDataCategory,
        parent_block_code: str | None,
        parent_frame_code: str | None,
        add_category_info: bool,
        add_item_info: bool,
    ) -> list:

        errs = []

        catdef = self["category"].get(cat.code)
        if catdef is None:
            errs.append(
                self._err(
                    type="undefined_category",
                    block=parent_block_code,
                    frame=parent_frame_code,
                    category=cat.code,
                )
            )
        else:
            # Check existence of mandatory items in category
            for mandatory_item_name in catdef["mandatory_items"]:
                if mandatory_item_name not in cat.item_names:
                    errs.append(
                        self._err(
                            type="missing_item",
                            block=parent_block_code,
                            frame=parent_frame_code,
                            category=cat.code,
                            item=mandatory_item_name,
                        )
                    )
            # Add category info
            if add_category_info:
                cat.description = catdef["description"]
                cat.groups = catdef["groups"]
                cat.keys = catdef["keys"]

        item_defs = {}
        for data_item in cat:
            itemdef = self["item"].get(data_item.name)
            if itemdef is None:
                errs.append(
                    self._err(
                        type="undefined_item",
                        block=parent_block_code,
                        frame=parent_frame_code,
                        category=cat.code,
                        item=data_item.code,
                    )
                )
            else:
                item_defs[data_item.code] = itemdef

        new_df, item_errs = self._validate_mmcif_category_table(
            block=parent_block_code,
            frame=parent_frame_code,
            category=cat.code,
            table=cat.df,
            item_defs=item_defs,
            caster=self._caster,
            case_normalization="lower",
        )
        errs.extend(item_errs)
        cat.df = new_df

        # Add item info
        if add_item_info:
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
        return errs

    def _validate_mmcif_category_table(
        self,
        block: str | None,
        frame: str | None,
        category: str | None,
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

        # Per spec: all values are strings or nulls.
        for name, dt in table.schema.items():
            if dt not in (pl.Utf8, pl.Null):
                raise TypeError(f"table column {name!r} must be Utf8 or Null; got {dt!r}")

        df = table.clone()
        errors: list[dict[str, Any]] = []

        # Temporary column naming: unique per (input_item, produced_output_name).
        TMP_PREFIX = "__tmp__"

        @dataclass(frozen=True)
        class Produced:
            """One produced (temporary) column emitted by one caster for one input item."""
            item: str
            out: str
            tmp: str
            plan: Any  # CastPlan (defined elsewhere)
            type_primitive: str
            type_code: str
            enum_vals_norm: list[str] | None
            ranges: list[tuple[float | None, float | None]] | None

        produced_by_name: dict[str, list[Produced]] = {}
        processed_items: set[str] = set()

        def _collect_rows(mask: pl.Expr) -> list[int]:
            # Eager: returns row indices where mask is True.
            return df.select(pl.arg_where(mask)).to_series(0).to_list()

        def _normalize_vals(vals: Sequence[str], mode: Literal["lower", "upper"]) -> list[str]:
            return [v.lower() for v in vals] if mode == "lower" else [v.upper() for v in vals]

        true_ci = {v.lower() for v in enum_true}
        false_ci = {v.lower() for v in enum_false}

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

        def _row_nullish_for_merge(col: pl.Expr, plan: Any) -> pl.Expr:
            """
            Nullish markers for Step 7 merge decision, at the ROW level.

            Per spec Step 7: null/NaN.
            Practical, unambiguous interpretation:
            - scalar float: null or NaN means "missing"
            - everything else (including containers): only null means "missing"
            (NaNs inside containers do not make the whole row "missing")
            """
            if plan.dtype == "float" and plan.container is None:
                return col.is_null() | col.is_nan()
            return col.is_null()

        def _any_violation(col: pl.Expr, plan: Any, pred_leaf: Callable[[pl.Expr], pl.Expr]) -> pl.Expr:
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

        def _map_leaves(col: pl.Expr, plan: Any, mapper: Callable[[pl.Expr], pl.Expr]) -> pl.Expr:
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

        def _allowed_by_ranges(el: pl.Expr, ranges: list[tuple[float | None, float | None]]) -> pl.Expr:
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

        # ---------------------------------------------------------------------
        # 1–6) Per input item: missing/default, regex, normalization, cast, enum/range checks
        # ---------------------------------------------------------------------
        for item, idef in item_defs.items():
            if item not in df.columns:
                # Item not present in table; skip.
                # Missing mandatory items are handled upstream.
                continue

            processed_items.add(item)

            default = idef.get("default")
            enum = list(idef.get("enumeration", {}).keys())
            ranges = idef.get("range")
            type_code = idef["type"]
            type_prim = idef["type_primitive"]
            type_regex = idef["type_regex"]

            col = pl.col(item).cast(pl.Utf8, strict=False)

            # -----------------------------------------------------------------
            # Step 1: Replace missing "?" with default, else record missing_value and set to null
            # -----------------------------------------------------------------
            miss = col == pl.lit("?")
            if default is not None:
                df = df.with_columns(
                    pl.when(miss).then(pl.lit(str(default))).otherwise(col).alias(item)
                )
            else:
                miss_rows = _collect_rows(miss)
                if miss_rows:
                    errors.append(
                        self._err(
                            type="missing_value",
                            block=block,
                            frame=frame,
                            category=category,
                            item=item,
                            column=item,
                            rows=miss_rows,
                        )
                    )
                df = df.with_columns(
                    pl.when(miss).then(pl.lit(None)).otherwise(col).alias(item)
                )

            col = pl.col(item).cast(pl.Utf8, strict=False)

            # -----------------------------------------------------------------
            # Step 2: Construct regex check (ignore null and ".")
            # -----------------------------------------------------------------
            check = col.is_not_null() & (col != pl.lit("."))
            construct_bad = check & (~col.str.contains(f"^(?:{type_regex})$"))
            bad_rows = _collect_rows(construct_bad)
            if bad_rows:
                errors.append(
                    self._err(
                        type="regex_violation",
                        block=block,
                        frame=frame,
                        category=category,
                        item=item,
                        column=item,
                        rows=bad_rows,
                    )
                )

            # -----------------------------------------------------------------
            # Step 3: Case normalization for "uchar"
            # -----------------------------------------------------------------
            if type_prim == "uchar" and case_normalization is not None:
                df = df.with_columns(
                    (col.str.to_lowercase() if case_normalization == "lower" else col.str.to_uppercase()).alias(item)
                )
                col = pl.col(item).cast(pl.Utf8, strict=False)

            # -----------------------------------------------------------------
            # Step 4: Cast via caster -> produce temp columns (unique per (item,out))
            # -----------------------------------------------------------------
            plans = caster(col, type_code)
            if not isinstance(plans, list) or not plans:
                raise TypeError(f"caster must return a non-empty list[CastPlan] for item {item!r}")

            # Normalize enum values (per item) if needed.
            enum_vals_norm: list[str] | None = None
            bool_like = False
            allowed_ci: set[str] | None = None

            if enum:
                if not isinstance(enum, list) or not all(isinstance(v, str) for v in enum):
                    raise TypeError(f'item_defs[{item!r}]["enum"] must be list[str] or None')
                enum_vals_norm = list(enum)
                if type_prim == "uchar" and case_normalization is not None:
                    enum_vals_norm = _normalize_vals(enum_vals_norm, case_normalization)

                enum_ci = {v.lower() for v in enum_vals_norm}
                bool_like = enum_to_bool and len(enum_ci) > 0 and enum_ci.issubset(true_ci | false_ci)
                allowed_ci = enum_ci if bool_like else None

            if ranges is not None and not isinstance(ranges, list):
                raise TypeError(f'item_defs[{item!r}]["range"] must be list[...] or None')

            # Produce per-item temp columns
            outs_seen: set[str] = set()
            tmp_exprs: list[pl.Expr] = []
            produced_entries: list[Produced] = []

            for p in plans:
                out = item if p.suffix == "" else f"{item}{p.suffix}"
                if out in outs_seen:
                    raise ValueError(f"caster produced duplicate output name {out!r} for item {item!r}")
                outs_seen.add(out)

                tmp_name = f"{TMP_PREFIX}{item}__{out}"
                tmp_exprs.append(p.expr.alias(tmp_name))

                produced_entries.append(
                    Produced(
                        item=item,
                        out=out,
                        tmp=tmp_name,
                        plan=p,
                        type_primitive=type_prim,
                        type_code=type_code,
                        enum_vals_norm=enum_vals_norm,
                        ranges=ranges,
                    )
                )

            df = df.with_columns(tmp_exprs)

            # -----------------------------------------------------------------
            # Step 5: Enum validation + conversion (only on "main" outputs)
            # -----------------------------------------------------------------
            if enum_vals_norm is not None:
                for prod in produced_entries:
                    if not prod.plan.main:
                        continue

                    tmp_col = pl.col(prod.tmp)
                    plan = prod.plan

                    if bool_like:
                        # Validate membership in the (case-insensitive) allowed set.
                        assert allowed_ci is not None

                        def pred(el: pl.Expr) -> pl.Expr:
                            n = _leaf_nullish_for_validation(el, plan)
                            return (~n) & (~el.str.to_lowercase().is_in(list(allowed_ci)))

                        viol = _any_violation(tmp_col, plan, pred)
                        viol_rows = _collect_rows(viol)
                        if viol_rows:
                            errors.append(
                                self._err(
                                    type="enum_violation",
                                    block=block,
                                    frame=frame,
                                    category=category,
                                    item=item,
                                    column=prod.out,
                                    rows=viol_rows,
                                )
                            )
                        else:
                            # Convert leaves to boolean (case-insensitive).
                            def mapper(el: pl.Expr) -> pl.Expr:
                                n = _leaf_nullish_for_validation(el, plan)
                                ci = el.str.to_lowercase()
                                mapped = (
                                    pl.when(ci.is_in(list(true_ci))).then(pl.lit(True))
                                    .when(ci.is_in(list(false_ci))).then(pl.lit(False))
                                    .otherwise(pl.lit(None))
                                )
                                return pl.when(n).then(pl.lit(None)).otherwise(mapped)

                            df = df.with_columns(_map_leaves(tmp_col, plan, mapper).alias(prod.tmp))

                    else:
                        # Enum values are strings => leaf must be "str".
                        if plan.dtype != "str":
                            raise TypeError(
                                f"Enum specified for item {item!r}, but main produced column {prod.out!r} "
                                f"has leaf dtype {plan.dtype!r}"
                            )

                        enum_dtype = pl.Enum(enum_vals_norm)

                        def pred(el: pl.Expr) -> pl.Expr:
                            n = _leaf_nullish_for_validation(el, plan)
                            return (~n) & (~el.is_in(enum_vals_norm))

                        viol = _any_violation(tmp_col, plan, pred)
                        viol_rows = _collect_rows(viol)
                        if viol_rows:
                            errors.append(
                                self._err(
                                    type="enum_violation",
                                    block=block,
                                    frame=frame,
                                    category=category,
                                    item=item,
                                    column=prod.out,
                                    rows=viol_rows,
                                )
                            )
                        else:
                            # Convert leaves to Enum while preserving nullish leaves.
                            def mapper(el: pl.Expr) -> pl.Expr:
                                n = _leaf_nullish_for_validation(el, plan)
                                return pl.when(n).then(el).otherwise(el.cast(enum_dtype))

                            df = df.with_columns(_map_leaves(tmp_col, plan, mapper).alias(prod.tmp))

            # -----------------------------------------------------------------
            # Step 6: Range validation (only on numeric items; only on "main" outputs)
            # -----------------------------------------------------------------
            if ranges is not None:
                if type_prim != "numb":
                    raise TypeError(
                        f"Range specified for non-numeric item {item!r} (type_primitive={type_prim!r})"
                    )

                for prod in produced_entries:
                    if not prod.plan.main:
                        continue

                    plan = prod.plan
                    if plan.dtype not in ("float", "int"):
                        raise TypeError(
                            f"Range specified for item {item!r}, but produced column {prod.out!r} "
                            f"has leaf dtype {plan.dtype!r}"
                        )

                    tmp_col = pl.col(prod.tmp)

                    def pred(el: pl.Expr) -> pl.Expr:
                        n = _leaf_nullish_for_validation(el, plan)
                        return (~n) & (~_allowed_by_ranges(el, ranges))

                    viol = _any_violation(tmp_col, plan, pred)
                    viol_rows = _collect_rows(viol)
                    if viol_rows:
                        errors.append(
                            self._err(
                                type="range_violation",
                                block=block,
                                frame=frame,
                                category=category,
                                item=item,
                                column=prod.out,
                                rows=viol_rows,
                            )
                        )

            # Record all produced temp columns for this input item
            for prod in produced_entries:
                produced_by_name.setdefault(prod.out, []).append(prod)

        # ---------------------------------------------------------------------
        # Step 7: Final replacement / merging
        #
        # Model:
        # - Every input item contributes one or more produced columns (temp).
        # - Group by output name:
        #     * unique output name: direct
        #     * repeated output name: merge with Step 7 semantics
        #
        # Critical detail:
        # - For repeated names, the "casted original" column should take precedence when non-nullish.
        #   We implement this by ordering producers so that (item == out) comes first.
        # ---------------------------------------------------------------------
        final_outs = set(produced_by_name.keys())
        out_exprs: list[pl.Expr] = []

        for out, prods in produced_by_name.items():
            # Prefer the producer that casts the original column itself (item == out),
            # so that "original non-nullish" overrides other produced values.
            prods_sorted = sorted(prods, key=lambda p: (p.item != out))

            if len(prods_sorted) == 1:
                out_exprs.append(pl.col(prods_sorted[0].tmp).alias(out))
                continue

            # Enforce merge compatibility: dtype + container must match across producers.
            first = prods_sorted[0]
            for p in prods_sorted[1:]:
                if (p.plan.dtype, p.plan.container) != (first.plan.dtype, first.plan.container):
                    raise TypeError(
                        f"Cannot merge repeated output column {out!r}: incompatible dtype/container "
                        f"{(first.plan.dtype, first.plan.container)} vs {(p.plan.dtype, p.plan.container)}"
                    )

            # Mismatch attribution: if there's a "self" producer, attribute lookups to that item name; else first producer.
            mismatch_item = out if any(p.item == out for p in prods_sorted) else first.item

            # Fold merge left-to-right:
            # - If current merged value is null/NaN => take next
            # - Else if next is non-nullish and differs => record auxiliary_mismatch
            merged = pl.col(first.tmp)
            merged_nullish = _row_nullish_for_merge(merged, first.plan)

            for nxt in prods_sorted[1:]:
                nxt_col = pl.col(nxt.tmp)
                nxt_nullish = _row_nullish_for_merge(nxt_col, nxt.plan)

                # Detect discrepancies where both are present.
                if first.plan.dtype == "float" and first.plan.container is None:
                    both_nan = merged.is_nan() & nxt_col.is_nan()
                    equal = (merged == nxt_col) | both_nan
                else:
                    equal = merged == nxt_col

                mismatch = (~merged_nullish) & (~nxt_nullish) & (~equal)
                mismatch_rows = _collect_rows(mismatch)
                if mismatch_rows:
                    errors.append(
                        self._err(
                            type="auxiliary_mismatch",
                            block=block,
                            frame=frame,
                            category=category,
                            item=mismatch_item,
                            column=out,
                            rows=mismatch_rows,
                        )
                    )

                # Merge rule: fill missing (nullish) values from nxt, otherwise keep current.
                merged = pl.when(merged_nullish).then(nxt_col).otherwise(merged)
                merged_nullish = _row_nullish_for_merge(merged, first.plan)

            out_exprs.append(merged.alias(out))

        # Materialize all final outputs
        df = df.with_columns(out_exprs)

        # Drop all temp columns
        tmp_cols = [c for c in df.columns if c.startswith(TMP_PREFIX)]
        if tmp_cols:
            df = df.drop(tmp_cols)

        # Drop processed raw input columns that are not in the final outputs.
        # (If a processed item is not produced with suffix "", it gets removed.)
        drop_cols = [c for c in processed_items if c in df.columns and c not in final_outs]
        if drop_cols:
            df = df.drop(drop_cols)

        return df, errors

    @staticmethod
    def _err(
        type: Literal[
            "undefined_category",
            "undefined_item",
            "missing_category",
            "missing_item",
            "missing_value",
            "regex_violation",
            "enum_violation",
            "range_violation",
            "auxiliary_mismatch",
        ],
        block: str | None = None,
        frame: str | None = None,
        category: str | None = None,
        item: str | None = None,
        column: str | None = None,
        rows: list[int] | None = None,
    ) -> dict[str, Any]:
        """Create an error dictionary."""
        return {
            "type": type,
            "block": block,
            "frame": frame,
            "category": category,
            "item": item,
            "column": column,
            "rows": rows,
        }


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
