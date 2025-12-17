"""DDL2 validator."""

import polars as pl

from ciffile.structure import CIFFile, CIFBlock, CIFDataCategory
from ._validator import CIFFileValidator


class DDL2Validator(CIFFileValidator):
    """DDL2 validator for CIF files."""

    def __init__(
        self,
        dictionary: CIFFile | CIFBlock,
    ) -> None:
        if isinstance(dictionary, CIFFile):
            if len(dictionary) != 1:
                raise ValueError(
                    "DDL2Validator requires a CIFFile with exactly one block as dictionary."
                )
            self._dict = dictionary[0]
        elif isinstance(dictionary, CIFBlock):
            self._dict = dictionary
        else:
            raise TypeError(
                "dictionary must be a CIFFile or CIFBlock instance."
            )

        self._catdict: dict[str, CIFDataCategory] = self._dict.part("dict_cat").category()
        self._keydict: dict[str, CIFDataCategory] = self._dict.part("dict_key").category()

        self._dict_title: str | None = None
        self._dict_description: str | None = None
        self._dict_version: str | None = None
        self._sub_category: dict[str, str] | None = None
        self._cat_group_description: dict[str, str] | None = None
        self._cat_group_parent: dict[str, str] | None = None

        self._canonical_name: dict[str, str] | None = None
        return

    @property
    def dict_title(self) -> str:
        """Title of the dictionary."""
        if self._dict_title is None:
            self._dict_title = str(self._dict["dictionary"]["title"][0])
        return self._dict_title

    @property
    def dict_description(self) -> str:
        """Description of the dictionary."""
        if self._dict_description is None:
            self._dict_description = self._normalize_whitespace(self._dict["datablock"]["description"][0])
        return self._dict_description

    @property
    def dict_version(self) -> str:
        """Version of the dictionary."""
        if self._dict_version is None:
            self._dict_version = str(self._dict["dictionary"]["version"][0])
        return self._dict_version

    @property
    def sub_category(self) -> dict[str, str]:
        """Mapping of [sub-category](https://www.iucr.org/__data/iucr/cifdic_html/2/mmcif_ddl.dic/Csub_category.html) IDs to their descriptions."""
        if self._sub_category is None:
            df = self._normalize_whitespace_in_column(
                self._dict["sub_category"].df,
                "description",
            )
            self._sub_category = dict(zip(df["id"], df["description"]))
        return self._sub_category

    @property
    def category_group_description(self) -> dict[str, str]:
        """Mapping of [category-group](https://www.iucr.org/__data/iucr/cifdic_html/2/mmcif_ddl.dic/Ccategory_group_list.html) IDs to their descriptions."""
        if self._cat_group_description is None:
            # Some dictionaries (e.g., mmcif_pdbx.dic) have duplicate definitions
            # for category groups. We keep only the first definition here.
            df = self._dict["category_group_list"].df.unique(subset="id", keep="first")
            df = self._normalize_whitespace_in_column(df, "description")
            self._cat_group_description = dict(zip(df["id"], df["description"]))
            self._cat_group_parent = dict(zip(df["id"], df["parent_id"]))
        return self._cat_group_description

    @property
    def category_group_parent(self) -> dict[str, str]:
        """Mapping of [category-group](https://www.iucr.org/__data/iucr/cifdic_html/2/mmcif_ddl.dic/Ccategory_group_list.html) IDs to their parent IDs."""
        if self._cat_group_parent is None:
            # Invoke category_group_description to populate _cat_group_parent
            _ = self.category_group_description
        return self._cat_group_parent

    @property
    def canonical_name(self) -> dict[str, str]:
        """Mapping of data item aliases to their canonical data names."""
        if self._canonical_name is None:
            df = self._catdict["item_aliases"].df
            self._canonical_name = dict(zip(df["alias_name"], df["_frame"]))
        return self._canonical_name

    def validate(
        self,
        file: CIFFile | CIFBlock | CIFDataCategory,
        add_category_info: bool = True,
    ) -> CIFFile | CIFBlock | CIFDataCategory:
        def validate_category(category: CIFDataCategory) -> None:
            return self._validate_category(
                category,
                add_category_info=add_category_info,
            )

        if isinstance(file, CIFDataCategory):
            validate_category(file)
            return file

        if isinstance(file, CIFBlock):
            is_block = True
            blocks = [file]
        else:
            is_block = False
            blocks = file

        for block in blocks:
            for frame in block.frames:
                for frame_category in frame:
                    validate_category(frame_category)
            for block_category in block:
                validate_category(block_category)
        return file[0] if is_block else file

    def _validate_category(
        self,
        cat: CIFDataCategory,
        add_category_info: bool,
    ) -> None:
        catdef = self._dict.frames[cat.code]

        # Add category info
        if add_category_info:
            # Set category description
            cat.description = self._normalize_whitespace(
                catdef["category"]["description"][0]
            )
            # Set category group info
            cat.groups = {
                group_id: self.category_group_description.get(group_id, "")
                for group_id in catdef["category_group"]["id"]
            }


    @staticmethod
    def _normalize_whitespace(text: str) -> str:
        return " ".join(text.split())

    @staticmethod
    def _normalize_whitespace_in_column(
        df: pl.DataFrame,
        column: str | list[str],
    ) -> pl.DataFrame:
        """Normalize whitespace in DataFrame column(s).

        This replaces all sequences of whitespace characters (including newlines)
        with a single space and trims leading/trailing whitespace.

        Parameters
        ----------
        df
            Input DataFrame.
        column
            Column name or list of column names to normalize.

        Returns
        -------
        DataFrame
            DataFrame with normalized whitespace in specified column(s).
        """
        return df.with_columns(
            pl.col(column)
            .str.replace_all(r"\s*(?:\r\n|\n|\r)\s*", " ")
            .str.strip_chars()
        )
