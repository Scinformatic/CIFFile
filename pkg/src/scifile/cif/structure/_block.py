"""CIF block data structure."""

from typing import Literal, Iterator, Callable, Sequence, Self

import polars as pl

from scifile.typing import DataFrameLike
from ._skel import CIFFileSkeleton
from ._block_like import CIFBlockLike
from ._util import extract_categories
from ._category import CIFDataCategory
from ._frame import CIFFrame


class CIFBlockFrames:
    def __init__(
        self,
        df: pl.DataFrame,
        *,
        variant: Literal["cif1", "mmcif"],
        col_name_frame: str | None,
        col_name_cat: str,
        col_name_key: str,
        col_name_values: str,
    ):
        self._df = df
        self._has_frames = col_name_frame is not None
        self._variant = variant
        self._col_frame = col_name_frame or ""
        self._col_cat = col_name_cat
        self._col_key = col_name_key
        self._col_values = col_name_values

        self._codes: pl.Series | None = None
        self._frames: dict[str, CIFFrame] = {}
        return

    @property
    def codes(self) -> pl.Series:
        """Unique frame codes in the data block."""
        if self._codes is None:
            self._codes = (
                self._df[self._col_frame].unique()
                if self._has_frames else
                pl.Series([], dtype=pl.Utf8)
            )
        return self._codes

    def write(
        self,
        writer: Callable[[str], None],
        *,
        # String casting parameters
        bool_true: str = "YES",
        bool_false: str = "NO",
        null_str: Literal[".", "?"] = "?",
        null_float: Literal[".", "?"] = "?",
        null_int: Literal[".", "?"] = "?",
        null_bool: Literal[".", "?"] = "?",
        empty_str: Literal[".", "?"] = ".",
        nan_float: Literal[".", "?"] = ".",
        # Styling parameters
        always_table: bool = False,
        list_style: Literal["horizontal", "tabular", "vertical"] = "tabular",
        table_style: Literal["horizontal", "tabular-horizontal", "tabular-vertical", "vertical"] = "tabular-horizontal",
        space_items: int = 2,
        min_space_columns: int = 2,
        indent: int = 0,
        indent_inner: int = 0,
        delimiter_preference: Sequence[Literal["single", "double", "semicolon"]] = ("single", "double", "semicolon"),
    ) -> None:
        """Write all save frames in the data block to the writer."""
        for frame in self():
            frame.write(
                writer,
                bool_true=bool_true,
                bool_false=bool_false,
                null_str=null_str,
                null_float=null_float,
                null_int=null_int,
                null_bool=null_bool,
                empty_str=empty_str,
                nan_float=nan_float,
                always_table=always_table,
                list_style=list_style,
                table_style=table_style,
                space_items=space_items,
                min_space_columns=min_space_columns,
                indent=indent,
                indent_inner=indent_inner,
                delimiter_preference=delimiter_preference,
            )
        return

    def __call__(self) -> Iterator[CIFFrame]:
        """Iterate over save frames in the data block."""
        for code in self.codes:
            yield self[code]

    def __iter__(self) -> Iterator[str]:
        """Iterate over frame codes in the data block."""
        for code in self.codes:
            yield code

    def __getitem__(
        self,
        frame_id: str | int | tuple[str | int, ...] | slice[int]
    ) -> CIFFrame | list[CIFFrame]:
        """Get a save frame by its frame code or index."""
        if isinstance(frame_id, str | int):
            frame_id = (frame_id,)
            single = True
        else:
            single = False

        if isinstance(frame_id, tuple):
            codes = [
                self.codes[cat_id]
                if isinstance(cat_id, int)
                else cat_id
                for cat_id in frame_id
            ]
        elif isinstance(frame_id, slice):
            codes = self.codes[frame_id].to_list()
        else:
            raise TypeError("frame_id must be str, int, tuple, or slice")

        frames = self._get_frames()
        out = [frames[k] for k in codes]

        if single:
            return out[0]
        return out

    def __len__(self) -> int:
        """Number of save frames in the data block."""
        return self.codes.shape[0]

    def _get_frames(self) -> dict[str, CIFFrame]:
        """Load all save frames in the data block."""
        if self._frames or not self._has_frames:
            return self._frames
        self._frames = {
            key[0]: CIFFrame(
                code=key[0],
                content=df,
                variant=self._variant,
                validate=False,
                col_name_cat=self._col_cat,
                col_name_key=self._col_key,
                col_name_values=self._col_values,
            )
            for key, df in self._df.partition_by(
                self._col_frame,
                include_key=False,
                as_dict=True,
            ).items()
        }
        return self._frames


class CIFBlock(CIFFileSkeleton, CIFBlockLike):
    """CIF file data block."""

    def __init__(
        self,
        code: str,
        content: DataFrameLike,
        *,
        variant: Literal["cif1", "mmcif"],
        validate: bool,
        col_name_frame: str | None,
        col_name_cat: str,
        col_name_key: str,
        col_name_values: str,
    ):
        super().__init__(
            code=code,
            content=content,
            variant=variant,
            validate=validate,
            require_block=False,
            require_frame=False,
            col_name_block=None,
            col_name_frame=col_name_frame,
            col_name_cat=col_name_cat,
            col_name_key=col_name_key,
            col_name_values=col_name_values,
        )
        self._frames: CIFBlockFrames | None = None
        return

    @property
    def frame_codes(self) -> pl.Series:
        """Unique frame codes in the data block."""
        return self.frames.codes

    @property
    def frames(self) -> CIFBlockFrames:
        """Save frames in the data block."""
        if self._frames is None:
            self._frames = CIFBlockFrames(
                df=self._get_part("dict"),
                variant=self._variant,
                col_name_frame=self._col_frame,
                col_name_cat=self._col_cat,
                col_name_key=self._col_key,
                col_name_values=self._col_values,
            )
        return self._frames

    def write(
        self,
        writer: Callable[[str], None],
        *,
        # String casting parameters
        bool_true: str = "YES",
        bool_false: str = "NO",
        null_str: Literal[".", "?"] = "?",
        null_float: Literal[".", "?"] = "?",
        null_int: Literal[".", "?"] = "?",
        null_bool: Literal[".", "?"] = "?",
        empty_str: Literal[".", "?"] = ".",
        nan_float: Literal[".", "?"] = ".",
        # Styling parameters
        always_table: bool = False,
        list_style: Literal["horizontal", "tabular", "vertical"] = "tabular",
        table_style: Literal["horizontal", "tabular-horizontal", "tabular-vertical", "vertical"] = "tabular-horizontal",
        space_items: int = 2,
        min_space_columns: int = 2,
        indent: int = 0,
        indent_inner: int = 0,
        delimiter_preference: Sequence[Literal["single", "double", "semicolon"]] = ("single", "double", "semicolon"),
    ) -> None:
        """Write all save frames in the data block to the writer."""
        space = " " * indent
        writer(f"{space}data_{self.code}\n")
        for category in self.categories():
            category.write(
                writer,
                bool_true=bool_true,
                bool_false=bool_false,
                null_str=null_str,
                null_float=null_float,
                null_int=null_int,
                null_bool=null_bool,
                empty_str=empty_str,
                nan_float=nan_float,
                always_table=always_table,
                list_style=list_style,
                table_style=table_style,
                space_items=space_items,
                min_space_columns=min_space_columns,
                indent=indent + indent_inner,
                indent_inner=indent_inner,
                delimiter_preference=delimiter_preference,
            )
        self.frames.write(
            writer,
            bool_true=bool_true,
            bool_false=bool_false,
            null_str=null_str,
            null_float=null_float,
            null_int=null_int,
            null_bool=null_bool,
            empty_str=empty_str,
            nan_float=nan_float,
            always_table=always_table,
            list_style=list_style,
            table_style=table_style,
            space_items=space_items,
            min_space_columns=min_space_columns,
            indent=indent + indent_inner,
            indent_inner=indent_inner,
            delimiter_preference=delimiter_preference,
        )
        return

    def new(
        self,
        code: str | None = None,
        content: DataFrameLike | None = None,
        *,
        variant: Literal["cif1", "mmcif"] | None = None,
        validate: bool | None = None,
        col_name_frame: str | None = None,
        col_name_cat: str | None = None,
        col_name_key: str | None = None,
        col_name_values: str | None = None,
    ) -> Self:
        """Create a new `CIFBlock` with modified parameters.

        Create a new `CIFBlock` object based on this one,
        but with some parameters modified.
        If an argument is `None`,
        the corresponding attribute of self is used.

        Parameters
        ----------
        code
            Block code for the new CIFBlock.
        content
            Content DataFrame for the new CIFBlock.
        variant
            CIF variant for the new CIFBlock.
        validate
            Whether to validate the content DataFrame for the new CIFBlock.
        col_name_frame
            Name of the column for frame codes in the new CIFBlock.
        col_name_cat
            Name of the column for category codes in the new CIFBlock.
        col_name_key
            Name of the column for data item keys in the new CIFBlock.
        col_name_values
            Name of the column for data item values in the new CIFBlock.
        """
        return CIFBlock(
            code=self.code if code is None else code,
            content=self._df if content is None else content,
            variant=self._variant if variant is None else variant,
            validate=(False if content is None else True) if validate is None else validate,
            col_name_frame=self._col_frame if col_name_frame is None else col_name_frame,
            col_name_cat=self._col_cat if col_name_cat is None else col_name_cat,
            col_name_key=self._col_key if col_name_key is None else col_name_key,
            col_name_values=self._col_values if col_name_values is None else col_name_values,
        )

    def __repr__(self) -> str:
        return f"CIFBlock(code={self.code!r}, type={self.type!r}, variant={self._variant!r}, categories={len(self.category_codes)})"

    def _get_categories(self) -> dict[str, CIFDataCategory]:
        """Load all data categories directly in the data block."""
        if self._categories:
            return self._categories

        data_df = self._get_part("data")
        category_dfs, _, _ = extract_categories(
            df=data_df,
            col_name_block=None,
            col_name_frame=None,
            col_name_cat=self._col_cat,
            col_name_key=self._col_key,
            col_name_values=self._col_values,
        )
        for cat_name, table in category_dfs.items():
            category = CIFDataCategory(
                code=cat_name,
                content=table,
                variant=self._variant,
                col_name_block=None,
                col_name_frame=None,
            )
            self._categories[cat_name] = category

        return self._categories
