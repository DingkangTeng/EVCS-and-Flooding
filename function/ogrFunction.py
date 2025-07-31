import gc, time
from osgeo import ogr, gdal
from contextlib import contextmanager
from typing import Generator

# Creat a new field in gpkg
def creatField(layer: ogr.Layer, fieldName: str, fieldType: int, maxLength: int = 255) -> None:
    """
    Create a new OGR field definition with the given name and OGR field type.
    fieldType should be one of the ogr.OFT* constants, e.g., ogr.OFTString, ogr.OFTInteger, etc.
    """
    fieldDefn = ogr.FieldDefn(fieldName, fieldType)
    if fieldType == ogr.OFTString:
        fieldDefn.SetWidth(max(80, min(maxLength, 255)))
    layer.CreateField(fieldDefn)

    return

@contextmanager
def orgDatasets(path: str, openType: int = 0, close: bool = True) -> 'Generator[gdal.Dataset, None, None]':
    ds = None
    try:
        # Retray open when occupied.
        while True:
            try:
                ds = ogr.Open(path, openType) # 0 is read only, 1 is writable
            except:
                time.sleep(10)
                continue
            else:
                break
        if not isinstance(ds, gdal.Dataset):
            raise RuntimeError("Failed to open layer dataset: {}".format(path))
        yield ds
    except:
        close = True
        raise RuntimeError
    finally:
        if close and ds is not None:
            ds.Destroy()
            gc.collect()