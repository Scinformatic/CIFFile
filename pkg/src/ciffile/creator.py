
def create(filepath: str | Path):
    path = Path(filepath)
    return _filestruct.DDL2CIFFile(df=pl.read_parquet(path))
