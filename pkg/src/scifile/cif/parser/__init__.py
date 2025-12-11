from ._parser_todict import CIFToDictParser


def parse(cif_file: str) -> dict:
    return CIFToDictParser().parse(cif_file)
