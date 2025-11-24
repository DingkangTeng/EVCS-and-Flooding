import gc
import rasterio as rio
import numpy as np
from osgeo import gdal
from contextlib import contextmanager
from typing import Generator
from rasterio.windows import from_bounds, Affine, Window
from rasterio.io import DatasetReader, DatasetWriter, BufferedDatasetWriter

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

# @contextmanager
# def getRasterByRectangleBoundary(rasterPath: str, XMin: float, YMin: float, XMax: float, YMax: float) -> Generator[gdal.Dataset]:
#     memDs = False
#     # Aviod the problem that the vector are too short to get a rectangle
#     if XMin == XMax:
#         XMin -= 0.0000001
#         XMax += 0.0000001
#     if YMin == YMax:
#         YMin -= 0.0000001
#         YMax += 0.0000001
#     try:
#         # Get raster data withing the layer extent
#         warpOptions = gdal.WarpOptions(
#             format="MEM", # Use in-memory dataset
#             outputBounds=[XMin, YMin, XMax, YMax], # Set the extent to the layer
#             cropToCutline=True, # Crop the raster to the extent of the mask
#             dstNodata=0,
#             multithread=True,
#             dstSRS="EPSG:4326"
#         )
#         memDs = gdal.Warp('', rasterPath, options=warpOptions)
#         if not isinstance(memDs, gdal.Dataset):
#             raise RuntimeError("Failed to warp raster GDAL error: {}".format(gdal.GetLastErrorMsg()))
#         yield memDs
#     except Exception as e:
#         raise RuntimeError("Failed to excute gdal.Warp(). Exception: \n{}".format(e))
#     finally:
#         if memDs:
#             memDs.FlushCache()
#             memDs.Destroy()

@contextmanager
def getRasterByRectangleBoundary(
    rasterPath: str,
    XMin: float, YMin: float, XMax: float, YMax: float,
    windowBuffer: float = 0.01
) -> Generator[tuple[np.ndarray, Affine]]:
    # # Aviod the problem that the vector are too short to get a rectangle
    # if XMin == XMax:
    #     XMin -= 0.0000001
    #     XMax += 0.0000001
    # if YMin == YMax:
    #     YMin -= 0.0000001
    #     YMax += 0.0000001
    XMin -= windowBuffer
    XMax += windowBuffer
    YMin -= windowBuffer
    YMax += windowBuffer
    src = rio.open(rasterPath)
    try:
        window = from_bounds(XMin, YMin, XMax, YMax, src.transform)
        # if window.height < 1 or window.width < 1:
        #     row_off, col_off = int(window.row_off), int(window.col_off)
        #     window = Window(col_off, row_off, max(window.width, 1), max(window.height,1)) # type: ignore
        data = src.read(1, window=window)
        transform = src.window_transform(window)
        yield data, transform
    finally:
        src.close()