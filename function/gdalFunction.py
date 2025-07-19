import gc
from osgeo import gdal
from contextlib import contextmanager
from typing import Generator

@contextmanager
def gdalDatasets(path: str, close: bool = True) -> Generator[gdal.Dataset, None, None]:
    ds = gdal.Open(path)
    try:
        if not isinstance(ds, gdal.Dataset):
            raise RuntimeError("Failed to open raster dataset: {}".format(path))
        else:
            yield ds
    except:
            close = True
            raise RuntimeError
    finally:
        if close:
            ds.Destroy()
            gc.collect()