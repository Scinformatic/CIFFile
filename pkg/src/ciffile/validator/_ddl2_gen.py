"""DDL2 validator."""

import warnings

import polars as pl

from ciffile.structure import CIFFile, CIFBlock
from ciffile._helper import normalize_whitespace as nws
from ciffile.structure._util import dataframe_to_dict


class DDL2Generator:
    """DDL2 validator generator."""

    def __init__(self, dictionary: CIFFile | CIFBlock) -> None:
        if isinstance(dictionary, CIFFile):
            if len(dictionary) != 1:
                raise ValueError(
                    "DDL2Generator requires a CIFFile with exactly one block as dictionary."
                )
            self._dict = dictionary[0]
        elif isinstance(dictionary, CIFBlock):
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

        self._out = {}
        return

    def generate(self) -> dict:
        """Generate dictionary metadata."""
        dic = self._dict
        cat = self._gen_cat()
        out = {
            "title": dic.get("dictionary").get("title").value,
            "description": nws(dic.get("datablock").get("description").value or ''),
            "version": dic.get("dictionary").get("version").value,
            "category_group_list": self._gen_cat_group_list(),
            "item_type_list": self._gen_item_type_list(),
            "sub_category": self._gen_sub_cat(),
            "category": cat,
            "canonical_name": self._gen_item(cat),
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
            warnings.warn(
                "DDL2Generator: Dictionary missing category_group_list.",
                stacklevel=2,
            )
            return {}

        df = self._dict[key].df
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            warnings.warn(
                f"DDL2Generator: category_group_list missing columns: {', '.join(missing_cols)}.",
                stacklevel=2,
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
            warnings.warn(
                "DDL2Generator: Dictionary missing item_type_list.",
                stacklevel=2,
            )
            return {}

        df = self._dict[key].df
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            warnings.warn(
                f"DDL2Generator: item_type_list missing columns: {', '.join(missing_cols)}.",
                stacklevel=2,
            )
            return {}

        return dataframe_to_dict(
            nws("detail", df=df),
            ids="code",
            multi_row="first",
            multi_row_warn=True,
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
            warnings.warn(
                "DDL2Generator: Dictionary missing sub_category.",
                stacklevel=2,
            )
            return {}

        df = self._dict[key].df
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            warnings.warn(
                f"DDL2Generator: sub_category missing columns: {', '.join(missing_cols)}.",
                stacklevel=2,
            )
            return {}

        return dataframe_to_dict(
            nws("description", df=df),
            ids="id",
            single_col="dict",
            multi_row="first",
            multi_row_warn=True,
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
                "group_ids": cat.get("category_group").get("id").values.to_list(),
                "keys": cat.get("category_key").get("name").values.to_list(),
            }
        return out

    def _gen_item(self, cat: dict[str, dict]) -> dict[str, str]:
        """Generate data for data items in a category."""
        alias_to_canonical: dict[str, str] = {}
        for key in self._keydict.frames:
            item = key["item"]
            if "category_id" not in item:
                if len(item.df) == 1:
                    category_ids = [key.code.split(".", 1)[0]]
                else:
                    self._warn(
                        f"Data item definition {key.code} missing category_id field "
                        "and multiple items present. Skipping."
                    )
                    continue
            else:
                category_ids = item["category_id"].values.to_list()

            for name, category, mandatory in zip(
                item["name"].values,
                category_ids,
                item["mandatory_code"].values,
            ):
                name = name.lower()
                category = category.lower()
                try:
                    item_dict = cat[category].setdefault("item", {})
                except KeyError as e:
                    self._warn(
                        f"Data item definition {name} references undefined category {category}."
                    )
                    raise e
                item_name = name.removeprefix("_")
                item_id = item_name.split(".", 1)[1]
                item_type = key.get("item_type")
                if not item_type:
                    self._warn(
                        f"Data item definition {item_name} missing item_type field."
                    )
                item_dict[item_id] = {
                    "name": item_name,
                    "description": nws(key.get("item_description").get("description").value or ""),
                    "mandatory": mandatory.lower() == "yes",
                    "default": key.get("item_default").get("value").value,
                    "item_enumeration": key.get("item_enumeration").get("value").values.to_list(),
                    "sub_categories": key.get("item_sub_category").get("id").values.to_list(),
                    "type": {
                        "code": item_type.get("code").value,
                        "condition": key.get("item_type_conditions").get("code").value,
                    },
                    "range": [
                        [minimum, maximum] if minimum != maximum else minimum
                        for minimum, maximum in zip(
                            key["item_range"]["minimum"].values.replace(".", None).cast(pl.Float32).fill_null(float("-inf")).to_list(),
                            key["item_range"]["maximum"].values.replace(".", None).cast(pl.Float32).fill_null(float("inf")).to_list(),
                        )
                    ] if "item_range" in key else [],
                    "unit": key.get("item_units").get("code").value,
                }
            for name_alias in key.get("item_aliases").get("alias_name").values.to_list():
                alias_to_canonical[name_alias.removeprefix("_")] = item_name

        return alias_to_canonical

    def _warn(self, message: str) -> None:
        warnings.warn(
            f"DDL2Generator: {message}",
            stacklevel=2,
        )
        return
