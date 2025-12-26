"""DDL2 validator generator."""

from __future__ import annotations

from typing import TYPE_CHECKING

import warnings

import polars as pl


from ciffile._helper import normalize_whitespace as nws
from ciffile.structure._util import dataframe_to_dict

if TYPE_CHECKING:
    from ciffile.structure import CIFFile, CIFBlock, CIFFrame, CIFDataCategory


class DDL2Generator:
    """DDL2 validator generator."""

    def __init__(self, dictionary: CIFFile | CIFBlock) -> None:
        if dictionary.container_type == "file":
            if len(dictionary) != 1:
                raise ValueError(
                    "DDL2Generator requires a CIFFile with exactly one block as dictionary."
                )
            self._dict = dictionary[0]
        elif dictionary.container_type == "block":
            self._dict = dictionary
        else:
            raise TypeError(
                "dictionary must be a CIFFile or CIFBlock instance."
            )

        catdict = self._dict.part("dict_cat")
        if catdict is None:
            raise ValueError(
                "DDL2Generator: Dictionary CIFBlock missing category definitions."
            )
        keydict = self._dict.part("dict_key")
        if keydict is None:
            raise ValueError(
                "DDL2Generator: Dictionary CIFBlock missing data item definitions."
            )

        self._catdict: CIFBlock = catdict
        self._keydict: CIFBlock = keydict

        self._item_generator = {
            "item_aliases": self._gen_item_aliases,
            "item_default": self._gen_item_default,
            "item_description": self._gen_item_description,
            "item_enumeration": self._gen_item_enumeration,
            "item_linked": self._gen_item_linked,
            "item_range": self._gen_item_range,
            "item_sub_category": self._gen_item_sub_category,
            "item_type": self._gen_item_type,
            "item_type_conditions": self._gen_item_type_conditions,
            "item_units": self._gen_item_units,
        }

        self._out = {}
        return

    def generate(self) -> dict:
        """Generate dictionary metadata."""
        dic = self._dict
        cat = self._gen_cat()
        item = self._gen_item()
        out = {
            "title": dic.get("dictionary").get("title").value,
            "description": nws(str(dic.get("datablock").get("description").value) or ''),
            "version": dic.get("dictionary").get("version").value,
            "category_group": self._gen_cat_group_list(),
            "item_type": self._gen_item_type_list(),
            "sub_category": self._gen_sub_cat(),
            "category": cat,
            "item": item,
        }
        self._out = out

        return out

    def _gen_cat_group_list(self) -> dict[str, dict[str, str]]:
        """Generate data for [category_group_list](https://www.iucr.org/__data/iucr/cifdic_html/2/mmcif_ddl.dic/Ccategory_group_list.html).

        Returns
        -------
        {category_group_id: {"description": str, "parent_id": str | None}, ...}
            Mapping of category group IDs to their properties.
        """
        key = "category_group_list"
        required_cols = {"id", "description", "parent_id"}

        if key not in self._dict:
            self._warn("Dictionary missing category_group_list.")
            return {}

        df = self._dict[key].df
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            self._warn(
                f"category_group_list missing columns: {', '.join(missing_cols)}."
            )
            return {}

        df = self._dict["category_group_list"].df.with_columns(
            pl.col("parent_id").replace(".", None),
            nws(pl.col("description")),
        )
        return dataframe_to_dict(
            df,
            ids="id",
            # Some dictionaries (e.g., mmcif_pdbx.dic)
            # have duplicate definitions for category groups.
            # We keep only the first definition here and warn the user.
            multi_row="first",
            multi_row_warn=True,
            df_name=key,
        )

    def _gen_item_type_list(self) -> dict[str, dict[str, str]]:
        """Generate data for [item_type_list](https://www.iucr.org/__data/iucr/cifdic_html/2/mmcif_ddl.dic/Citem_type_list.html).

        Returns
        -------
        {item_type_code: {"primitive_code": str, "construct": str, "detail": str}, ...}
            Mapping of item type codes to their properties.
        """
        key = "item_type_list"
        required_cols = {"code", "primitive_code", "construct", "detail"}

        if key not in self._dict:
            self._warn(
                "Dictionary missing item_type_list."
            )
            return {}

        df = self._dict[key].df
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            self._warn(
                f"item_type_list missing columns: {', '.join(missing_cols)}."
            )
            return {}

        df = df.with_columns(
            pl.col("code").str.to_lowercase(),
        )

        return dataframe_to_dict(
            nws("detail", df=df).rename({"primitive_code": "primitive", "construct": "regex"}),
            ids="code",
            multi_row="first",
            multi_row_warn=True,
            df_name=key,
        )

    def _gen_sub_cat(self) -> dict[str, dict[str, str]]:
        """Generate data for [sub_category](https://www.iucr.org/__data/iucr/cifdic_html/2/mmcif_ddl.dic/Csub_category.html).

        Returns
        -------
        {sub_category_id: {"description": str}, ...}
            Mapping of sub-category IDs to their properties.
        """
        key = "sub_category"
        required_cols = {"id", "description"}

        if key not in self._dict:
            self._warn(
                "Dictionary missing sub_category."
            )
            return {}

        df = self._dict[key].df
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            self._warn(
                f"sub_category missing columns: {', '.join(missing_cols)}."
            )
            return {}

        return dataframe_to_dict(
            nws("description", df=df),
            ids="id",
            single_col="value",
            multi_row="first",
            multi_row_warn=True,
            df_name=key,
        )

    def _gen_cat(self) -> dict[str, dict]:
        """Generate data for categories."""
        out = {}
        for cat in self._catdict.frames:
            category = cat["category"]
            cat_id = cat["category"]["id"].value
            out[cat_id.lower()] = {
                "description": nws(category["description"].value),
                "mandatory": category["mandatory_code"].value.lower() == "yes",
                "groups": cat.get("category_group").get("id").values.to_list(),
                "keys": (
                    cat.get("category_key").get("name").values
                    .cast(pl.Utf8)
                    .str.to_lowercase()
                    .str.splitn(".", 2)
                    .struct.field("field_1")
                    .to_list()
                ),
            }
        return out

    def _gen_item(self) -> dict[str, dict]:
        """Generate data for data items in a category."""
        out_item = {}
        item_name_frame_contributors: dict[str, set[str]] = {}
        for item_frame in self._keydict.frames:

            if "item" not in item_frame:
                raise ValueError(f"Data item definition save frame '{item_frame.code}' missing 'item' category.")

            if "item_description" not in item_frame:
                raise ValueError(
                    f"Data item definition save frame '{item_frame.code}' missing 'item_description' category."
                )

            item_self, item_others = self._normalize_item_df(item_frame["item"].df, frame_code=item_frame.code)

            item_dict = {
                "category": item_self["category_id"].lower(),
                "mandatory": item_self["mandatory_code"] == "yes",
                "others": item_others
            }

            for key, gen_func in self._item_generator.items():
                if key in item_frame:
                    item_dict[key.removeprefix("item_")] = gen_func(item_frame[key], frame_code=item_frame.code)

            item_name = item_self["name"]
            out_item[item_name] = item_dict
            item_name_frame_contributors.setdefault(item_name, set()).add(item_frame.code)
        return self._fill_items(out_item, item_name_frame_contributors)

    def _fill_items(
        self,
        items: dict[str, dict],
        item_name_frame_contributors: dict[str, set[str]],
    ) -> dict[str, dict]:
        """Generate data for data items in a category."""
        def update(add_from: dict, add_to: dict, add_to_name: str, add_extra: dict | None = None) -> None:
            self._update_item_definition(
                add_from=add_from | (add_extra or {}),
                add_to=add_to,
                add_from_name=item_name,
                add_to_name=add_to_name,
                contributors=item_name_frame_contributors,
            )
            for key, value_to in add_to.items():
                if key not in {"aliases", "others", "description", "linked", "category", "mandatory"} and key not in add_from:
                    add_from[key] = value_to
            item_name_frame_contributors.setdefault(add_to_name, set()).add(item_name)
            return

        for item_name, item in items.items():

            other_items_df = item.pop("others")

            for row in other_items_df.iter_rows(named=True):
                extra = {
                    "category": row["category_id"].lower(),
                    "mandatory": row["mandatory_code"] == "yes",
                }
                other_item = items.setdefault(row["name"], {})
                update(item, other_item, row["name"], add_extra=extra)

            if "linked" in item:
                linked_names = item["linked"]
                added_names = {item_name} | set(other_items_df["name"].to_list())
                remaining_names = set(linked_names) - added_names
                for name in remaining_names:
                    out_item_linked = items.setdefault(name, {})
                    update(item, out_item_linked, name)

        return items

    def _warn(self, message: str) -> None:
        warnings.warn(message, stacklevel=3)
        return

    def _normalize_item_df(self, df: pl.DataFrame, frame_code: str) -> tuple[dict, pl.DataFrame]:

        if len(df) == 0:
            raise ValueError(f"Data item definition save frame '{frame_code}' has no data.")

        if not all(item_keyword in {"name", "mandatory_code", "category_id"} for item_keyword in df.columns):
            raise ValueError(
                f"Data item definition save frame '{frame_code}' has unexpected categories."
            )

        if "mandatory_code" not in df:
            raise ValueError(
                f"Data item definition '{frame_code}' missing mandatory_code field."
            )

        if not df["mandatory_code"].is_in(["yes", "no", "implicit", "implicit-ordinal"]).all():
            raise ValueError(
                f"Data item definition '{frame_code}' has invalid mandatory_code values."
            )

        frame_code_category, frame_code_keyword = frame_code.split(".", 1)

        # Normalize item names
        if "name" not in df:
            if len(df) == 1:
                df = df.with_columns(name=frame_code)
            else:
                raise ValueError(
                    f"Data item definition '{frame_code}' missing name field and multiple items present."
                )
        else:
            df = df.with_columns(
                name=(
                    pl.col("name")
                    .str.to_lowercase()
                    .str.strip_prefix("_")
                )
            )

        # Normalize item category IDs
        if "category_id" not in df:
            if len(df) == 1:
                df = df.with_columns(
                    category_id=pl.lit(frame_code_category)
                )
            else:
                raise ValueError(
                    f"Data item definition '{frame_code}' missing category_id field and multiple items present."
                )
        else:
            df = df.with_columns(
                category_id=pl.col("category_id").str.to_lowercase()
            )

        item_duplicated = df.is_duplicated()
        if item_duplicated.any():
            self._warn(
                f"Data item definition '{frame_code}' has duplicated item names."
            )
            # Select only the first occurrence of each duplicated item name
            df = df.unique()

        item_name_duplicated = df["name"].is_duplicated()
        if item_name_duplicated.any():
            raise ValueError(
                f"Data item definition '{frame_code}' has duplicated item names after normalization."
            )

        row_is_self = (df["name"] == frame_code) & (df["category_id"] == frame_code_category)
        if not row_is_self.any():
            raise ValueError(
                f"Data item definition '{frame_code}' missing definition for itself."
            )

        self_row = df.filter(row_is_self).row(0, named=True)
        other_rows = df.filter(~row_is_self)
        return self_row, other_rows

    def _update_item_definition(
        self,
        add_from: dict,
        add_to: dict,
        add_from_name: str,
        add_to_name: str,
        contributors: dict[str, set[str]],
    ) -> None:
        for key, value_from in add_from.items():
            if key in {"aliases", "others", "description", "linked", "category"}:
                continue
            if key not in add_to:
                add_to[key] = value_from
                continue
            value_to = add_to[key]
            if value_from == value_to:
                continue
            if key == "mandatory":
                continue
            self._warn(
                f"Conflicting definitions in '{key}' of data item '{add_to_name}' "
                f"while adding definitions from data item '{add_from_name}':\n"
                f"'{add_from_name}' defines: '{value_from!r}'\n"
                f"Existing definition is '{value_to!r}'\n"
                f"Other contributors to '{add_to_name}' are: {', '.join(f"'{c}'" for c in contributors.get(add_to_name, []))}"
            )
        return

    def _gen_item_aliases(self, item: CIFDataCategory, frame_code: str) -> list[dict[str, str]]:
        for mandatory_keyword in {"alias_name", "dictionary", "version"}:
            if mandatory_keyword not in item:
                raise ValueError(f"item_aliases missing mandatory keyword '{mandatory_keyword}'.")
        is_duplicated = item.df.is_duplicated()
        if is_duplicated.any():
            self._warn(
                f"Duplicated alias names found in frame code '{frame_code}' "
                "(in 'item_aliases.alias_name')."
            )
        return list(
            item.df
            .with_columns(pl.col("alias_name").str.to_lowercase().str.strip_prefix("_"))
            .rename({"alias_name": "name"})
            .iter_rows(named=True)
        )

    @staticmethod
    def _gen_item_description(item: CIFDataCategory, frame_code: str) -> str:
        if "description" not in item:
            raise ValueError(
                f"Data item definition missing 'description' in 'item_description' category."
            )
        if len(item["description"].values) != 1:
            raise ValueError(
                f"Data item definition 'item_description' must have exactly one 'description' value."
            )
        description = item["description"].values[0]
        if not isinstance(description, str):
            raise ValueError(
                f"Data item definition 'item_description' value must be a string."
            )
        return nws(description)

    @staticmethod
    def _gen_item_enumeration(item: CIFDataCategory, frame_code: str) -> dict[str, dict[str, str]]:
        if "value" not in item:
            raise ValueError("item_enumeration missing mandatory keyword 'value'.")
        if len(item) == 1:
            # Only 'value' column present
            return {val: {} for val in item["value"].values.to_list()}
        df = item.df
        if "detail" in df.columns:
            df = nws("detail", df=df)
        return dataframe_to_dict(
            df,
            ids="value",
            single_col="dict",
            multi_row="first",
            multi_row_warn=True,
            df_name=f"{frame_code}.item_enumeration",
        )

    @staticmethod
    def _gen_item_default(item: CIFDataCategory, frame_code: str) -> str:
        if "value" not in item:
            raise ValueError("item_default missing mandatory keyword 'value'.")
        if len(item["value"].values) != 1:
            raise ValueError("item_default must have exactly one 'value'.")
        return item["value"].values[0]

    @staticmethod
    def _gen_item_linked(item: CIFDataCategory, frame_code: str) -> list[str]:
        for mandatory_keyword in {"child_name", "parent_name"}:
            if mandatory_keyword not in item:
                raise ValueError(
                    f"item_linked missing mandatory keyword {mandatory_keyword}."
                )
        child_names, parent_names = (
            set(item[name].values.str.to_lowercase().str.strip_prefix("_").to_list())
            for name in ("child_name", "parent_name")
        )
        return list(child_names | parent_names)

    @staticmethod
    def _gen_item_range(item: CIFDataCategory, frame_code: str) -> list[tuple[float | None, float | None]]:
        for mandatory_keyword in {"minimum", "maximum"}:
            if mandatory_keyword not in item:
                raise ValueError(
                    f"item_range missing mandatory keyword {mandatory_keyword}."
                )
        out = [
            (minimum, maximum)
            for minimum, maximum in zip(
                item["minimum"].values.replace(".", None).cast(pl.Float32).to_list(),
                item["maximum"].values.replace(".", None).cast(pl.Float32).to_list(),
            )
        ]
        return sorted(
            out,
            key=lambda x: (
                float("-inf") if x[0] is None else x[0],
                float("inf") if x[1] is None else x[1]
            )
        )

    @staticmethod
    def _gen_item_sub_category(item: CIFDataCategory, frame_code: str) -> list[str]:
        if "id" not in item:
            raise ValueError("item_sub_category missing mandatory keyword 'id'.")
        return item["id"].values.sort().to_list()

    @staticmethod
    def _gen_item_type(item: CIFDataCategory, frame_code: str) -> str:
        if "code" not in item:
            raise ValueError("item_type missing mandatory keyword 'code'.")
        if len(item["code"].values) != 1:
            raise ValueError("item_type must have exactly one 'code' value.")
        return item["code"].values[0].lower()

    @staticmethod
    def _gen_item_type_conditions(item: CIFDataCategory, frame_code: str) -> list[str]:
        if "code" not in item:
            raise ValueError("item_type_conditions missing mandatory keyword 'code'.")
        codes = item["code"].values.str.to_lowercase().unique().sort()
        if not codes.is_in(["esd", "seq"]).all():
            raise ValueError(
                "item_type_conditions 'code' values must be 'esd' or 'seq' only."
            )
        return codes.to_list()

    @staticmethod
    def _gen_item_units(item: CIFDataCategory, frame_code: str) -> str:
        if "code" not in item:
            raise ValueError("item_units missing mandatory keyword 'code'.")
        if len(item["code"].values) != 1:
            raise ValueError("item_units must have exactly one 'code' value.")
        return item["code"].values[0].lower()
