"""Read CIF files."""

import polars as pl

from scifile.typing import FileLike
from .parser import parse
from .structure import CIFFile


def read(
    file: FileLike,
    encoding: str = "utf-8",
):
    columns, parsing_errors = parse(file=file, encoding=encoding)
    df = pl.DataFrame(
            columns,
            {
                "block_code": pl.Utf8,
                "frame_code_category": pl.Utf8,
                "frame_code_keyword": pl.Utf8,
                "data_name_category": pl.Utf8,
                "data_name_keyword": pl.Utf8,
                "data_values": pl.List(pl.Utf8),
                "loop_id": pl.UInt32,
            },
        )


    cif = CIFFile(df=df)
    return cif
