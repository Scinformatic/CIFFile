import numpy as np
import polars as pl

from .._df_tools import CIFItemCategoryDataFrame
from .._filestruct import DDL2CIFFile

mapping = {
    "data": {
        "category_group_list",
        "datablock",
        "dictionary",
        "dictionary_history",
        "item_type_list",
        "item_units_conversion",
        "item_units_list",
        "pdbx_comparison_operator_list",
        "pdbx_conditional_context_list",
        "pdbx_dictionary_component",
        "pdbx_dictionary_component_history",
        "pdbx_item_linked_group",
        "pdbx_item_linked_group_list",
        "sub_category",
    },
    "def_cat": {
        "category",
        "category_examples",
        "category_group",
        "category_key",
        "ndb_category_examples",
        "pdbx_category_conditional_context",
        "pdbx_category_context",
        "pdbx_category_description",
    },
    "def_key": {
        "item",
        "item_aliases",
        "item_default",
        "item_dependent",
        "item_description",
        "item_enumeration",
        "item_examples",
        "item_linked",
        "item_range",
        "item_related",
        "item_sub_category",
        "item_type",
        "item_type_conditions",
        "item_units",
        "pdbx_item",
        "pdbx_item_conditional_context",
        "pdbx_item_context",
        "pdbx_item_description",
        "pdbx_item_enumeration",
        "pdbx_item_enumeration_details",
        "pdbx_item_examples",
        "pdbx_item_range",
        "pdbx_item_type",
    },
}
