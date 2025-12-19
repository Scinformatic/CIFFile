"""DDL2 validator."""

import polars as pl

from ciffile.structure import CIFFile, CIFBlock, CIFDataCategory
from ._validator import CIFFileValidator


class DDL2Validator(CIFFileValidator):
    """DDL2 validator for CIF files."""

    def __init__(self, dictionary: dict) -> None:
        self._dict = dictionary
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

        canonical_names = self.canonical_name
        for data_item in cat:
            name = data_item.code
            canonical_name = canonical_names.get(name, name)
