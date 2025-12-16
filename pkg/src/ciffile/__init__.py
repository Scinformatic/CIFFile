"""CIFFile: Read, write, and validate Crystallographic Information Files ([CIF](https://en.wikipedia.org/wiki/Crystallographic_Information_File)).

Notes
-----
- Currently, only the [Version 1.1](https://www.iucr.org/resources/cif/spec/version1.1) format is supported.
  [Version 2.0](https://www.iucr.org/resources/cif/cif2) support is planned for future releases.

References
----------
Official resources:
- [IUCr](https://www.iucr.org/resources/cif)
- [CIF specification](https://www.iucr.org/resources/cif/spec)
- [CIF dictionaries](https://www.iucr.org/resources/cif/dictionaries)
- [CIF dictionary definition languages (DDl)](https://www.iucr.org/resources/cif/ddl)
- [CIF file syntax](https://www.iucr.org/resources/cif/spec/version1.1/cifsyntax)
- [Online CIF file validator](https://checkcif.iucr.org/)

Other resources:
- [Metadata Standards Catalog](https://rdamsc.bath.ac.uk/msc/m6)

Publications:
- https://doi.org/10.1107/97809553602060000728

Other Python packages with CIF/mmCIF support:
- [mmCIF Core Access Library (by RCSB)](https://github.com/rcsb/py-mmcif)
- [PDBeCIF (by PDBe)](https://github.com/PDBeurope/pdbecif)
- [BioPython](https://github.com/biopython/biopython/blob/master/Bio/PDB/MMCIFParser.py)
- [BioPandas](https://github.com/BioPandas/biopandas/tree/main/biopandas/mmcif)
- [Biotite](https://github.com/biotite-dev/biotite/tree/master/src/biotite/structure/io/pdbx)
"""

from .reader import read

__all__ = [
    "read",
]
