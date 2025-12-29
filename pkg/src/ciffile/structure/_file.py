"""CIF file data structure."""

from typing import Literal

from ciffile.typing import DataFrameLike
from ciffile.validation import dictionary as to_validator_dict
from ._block import CIFBlock
from ._base import CIFStructureWithFrame


class CIFFile(CIFStructureWithFrame[CIFBlock]):
    """CIF file."""

    def __init__(
        self,
        content: DataFrameLike,
        *,
        variant: Literal["cif1", "mmcif"],
        validate: bool,
        col_name_block: str,
        col_name_frame: str | None,
        col_name_cat: str,
        col_name_key: str,
        col_name_values: str,
        allow_duplicate_rows: bool = False,
        code: None = None,
    ):
        super().__init__(
            code=code,
            container_type="file",
            content=content,
            variant=variant,
            validate=validate,
            require_block=True,
            require_frame=False,
            col_name_block=col_name_block,
            col_name_frame=col_name_frame,
            col_name_cat=col_name_cat,
            col_name_key=col_name_key,
            col_name_values=col_name_values,
            allow_duplicate_rows=allow_duplicate_rows,
        )
        return

    def to_validator_dict(
        self,
        block: int | str = 0,
        *,
        variant: Literal["ddl2"] = "ddl2"
    ) -> dict:
        return to_validator_dict(self[block], variant=variant)

    def __repr__(self) -> str:
        """Representation of the CIF file."""
        return f"CIFFile(type={self.type!r}, variant={self._variant!r}, blocks={len(self)!r})"

    def _get_codes(self) -> list[str]:
        """Get codes of the data blocks in the CIF file."""
        return self._df[self._col_block].unique(maintain_order=True).to_list()

    def _get_elements(self) -> dict[str, CIFBlock]:
        """Load all data blocks in the CIF file."""
        return {
            key[0]: CIFBlock(
                code=key[0],
                content=df,
                variant=self._variant,
                validate=False,
                col_name_frame=self._col_frame,
                col_name_cat=self._col_cat,
                col_name_key=self._col_key,
                col_name_values=self._col_values,
            )
            for key, df in self.df.partition_by(
                self._col_block,
                include_key=False,
                as_dict=True,
            ).items()
        }

    def _get_empty_element(self) -> CIFBlock:
        return CIFBlock(
            code="",
            content=self.df.clear(),
            variant=self._variant,
            validate=False,
            col_name_frame=self._col_frame,
            col_name_cat=self._col_cat,
            col_name_key=self._col_key,
            col_name_values=self._col_values,
        )
