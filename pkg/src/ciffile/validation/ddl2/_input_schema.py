from typing import Literal

from pydantic import BaseModel


class DDL2CategoryGroup(BaseModel):
    """Representation of a DDL2 CIF category group."""

    parent_id: str | None
    description: str


class DDL2ItemType(BaseModel):
    """Representation of a DDL2 CIF item type.

    Attributes
    ----------
    primitive
        Primitive data type:
        - "char": Character-based data.
        - "uchar": Unsigned (case-insensitive) character-based data.
        - "numb": Numeric data.
    regex
        Regular expression defining the valid format for this data type.
    detail
        Optional detailed description of the data type.
    """

    primitive: Literal["char", "uchar", "numb"]
    regex: str
    detail: str | None


class DDL2CategoryDef(BaseModel):
    """Representation of a DDL2 CIF category definition."""

    description: str
    mandatory: bool
    groups: list[str]
    keys: list[str]


class DDL2ItemDef(BaseModel):
    """Representation of a DDL2 CIF data item definition.

    Attributes
    ----------
    category
        Name of the category this data item belongs to.
    description
        Description of the data item.
    mandatory
        Whether this data item is mandatory.
    type
        Data type code of this data item.
    type_conditions
        List of type condition codes applicable to this data item.
    aliases
        List of aliases for this data item.
    default
        Default value for this data item, if any.
    enumeration
        Mapping of allowed enumeration values to their definitions, if any.
    range
        List of allowed ranges for this data item, if any.
        Each range is a 2-tuple indicating an exclusive minimum and maximum value, respectively.
        A value of `None` for minimum or maximum indicates no bound in that direction.
        If both minimum and maximum are the same non-None float value,
        it indicates that only that exact value is allowed.
        The allowed range for the data item is the union of all specified ranges.
    sub_categories
        List of sub-category IDs this data item belongs to.
    units
        Units associated with this data item, if any.
    linked
        Set of data items linked to this data item, if any.
    """

    category: str
    description: str
    mandatory: bool
    type: str
    type_conditions: list[Literal["esd", "seq"]] | None = None
    aliases: list[dict[str, str]] | None = None
    default: str | None = None
    enumeration: dict[str, dict] | None = None
    range: list[tuple[float | None, float | None]] | None = None
    sub_categories: list[str] | None = None
    units: str | None = None
    linked: set[str] | None = None


class DDL2Dictionary(BaseModel):
    """Representation of a DDL2 CIF dictionary.

    Attributes
    ----------
    category
        Mapping of category names to their definitions.
    item
        Mapping of data item names to their definitions.
    category_group
        Mapping of category group names to their definitions.
    sub_category
        Mapping of sub-category names to their descriptions.
    item_type
        Mapping of DDL2 data type codes to their definitions.
    title
        Optional title of the dictionary.
    description
        Optional description of the dictionary.
    version
        Optional version of the dictionary.
    """

    category: dict[str, DDL2CategoryDef]
    item: dict[str, DDL2ItemDef]
    category_group: dict[str, DDL2CategoryGroup]
    sub_category: dict[str, str]
    item_type: dict[str, DDL2ItemType]
    title: str | None = None
    description: str | None = None
    version: str | None = None
