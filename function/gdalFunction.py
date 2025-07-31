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

@contextmanager
def getRasterByRectangleBoundary(rasterPath: str, XMin: float, YMin: float, XMax: float, YMax: float) -> Generator[gdal.Dataset]:
    memDs = False
    # Aviod the problem that the vector are too short to get a rectangle
    if XMin == XMax:
        XMin -= 0.0000001
        XMax += 0.0000001
    if YMin == YMax:
        YMin -= 0.0000001
        YMax += 0.0000001
    try:
        # Get raster data withing the layer extent
        warpOptions = gdal.WarpOptions(
            format="MEM", # Use in-memory dataset
            outputBounds=[XMin, YMin, XMax, YMax], # Set the extent to the layer
            cropToCutline=True, # Crop the raster to the extent of the mask
            dstNodata=0,
            multithread=True,
            dstSRS="EPSG:4326"
        )
        memDs = gdal.Warp('', rasterPath, options=warpOptions)
        if not isinstance(memDs, gdal.Dataset):
            raise RuntimeError("Failed to warp raster GDAL error: {}".format(gdal.GetLastErrorMsg()))
        yield memDs
    except Exception as e:
        raise RuntimeError("Failed to excute gdal.Warp(). Exception: \n{}".format(e))
    finally:
        if memDs:
            memDs.FlushCache()
            memDs.Destroy()