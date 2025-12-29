"""CIF file save frames data structure."""

from typing import Literal

from ciffile.typing import DataFrameLike
from ._base import CIFStructureWithFrame
from ._frame import CIFFrame


class CIFBlockFrames(CIFStructureWithFrame[CIFFrame]):
    """CIF file data block save frames."""

    def __init__(
        self,
        content: DataFrameLike,
        *,
        variant: Literal["cif1", "mmcif"],
        validate: bool = False,
        col_name_frame: str | None,
        col_name_cat: str,
        col_name_key: str,
        col_name_values: str,
        code: None = None,
        col_name_block: None = None,
        allow_duplicate_rows: bool = False,
    ):
        super().__init__(
            code=code,
            container_type="frames",
            content=content,
            variant=variant,
            validate=validate,
            require_block=False,
            require_frame=False,
            col_name_block=col_name_block,
            col_name_frame=col_name_frame,
            col_name_cat=col_name_cat,
            col_name_key=col_name_key,
            col_name_values=col_name_values,
            allow_duplicate_rows=allow_duplicate_rows,
        )

        self._has_frames = col_name_frame is not None
        self._col_frame = col_name_frame or ""
        return

    def __repr__(self) -> str:
        return f"CIFBlockFrames(variant={self._variant!r}, frames={len(self)})"

    def _get_codes(self) -> list[str]:
        """Get codes of the save frames in the data block."""
        return self._df[self._col_frame].unique(maintain_order=True).to_list() if self._has_frames else []

    def _get_elements(self) -> dict[str, CIFFrame]:
        """Load all save frames in the data block."""
        if not self._has_frames:
            return {}
        frames = {
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
        return frames

    def _get_empty_element(self) -> CIFFrame:
        return CIFFrame(
            code="",
            content=self.df.clear(),
            variant=self._variant,
            validate=False,
            col_name_cat=self._col_cat,
            col_name_key=self._col_key,
            col_name_values=self._col_values,
        )
