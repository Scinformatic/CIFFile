from typing import Literal

from pydantic import BaseModel


class DDL2CategoryGroup(BaseModel):
    """Representation of a DDL2 CIF category group."""

    parent_id: str | None
    description: str


class DDL2ItemType(BaseModel):
    """Representation of a DDL2 CIF item type."""

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
    """Representation of a DDL2 CIF data item definition."""

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


class DDL2Dictionary(BaseModel):
    """Representation of a DDL2 CIF dictionary."""

    title: str | None
    description: str | None
    version: str | None
    category_group: dict[str, DDL2CategoryGroup]
    item_type: dict[str, DDL2ItemType]
    sub_category: dict[str, str]
    category: dict[str, DDL2CategoryDef]
    item: dict[str, DDL2ItemDef]
