"""Crystallographic Information File ([CIF](https://en.wikipedia.org/wiki/Crystallographic_Information_File)).

Currently, only the [Version 1.1](https://www.iucr.org/resources/cif/spec/version1.1) format is supported.

References
----------
- [Official CIF specification](https://www.iucr.org/resources/cif/spec)
- [Metadata Standards Catalog](https://rdamsc.bath.ac.uk/msc/m6)

Publications:
- https://doi.org/10.1107/97809553602060000728

Other Python packages with mmCIF support:
- [mmCIF Core Access Library (by RCSB)](https://github.com/rcsb/py-mmcif)
- [BioPython](https://github.com/biopython/biopython/blob/master/Bio/PDB/MMCIFParser.py)
- [BioPandas](https://github.com/BioPandas/biopandas/tree/main/biopandas/mmcif)
- [Biotite](https://github.com/biotite-dev/biotite/tree/master/src/biotite/structure/io/pdbx)
"""

from .read import read

__all__ = [
    "read",
]
