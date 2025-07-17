import sys, gc
from osgeo import gdal, ogr, osr
from contextlib import contextmanager
from typing import Generator

sys.path.append(".") # Set path to the roots

class getPixelsValues:
    __slots__ = [
        "layerPath", "layerName", "layerRef",
        "rasterPath", "projection", "geotrans", "ref"
    ]
    
    def __init__(self, rasterPath: str | None = None, layer: str | tuple[str, str] | None = None):
        """
        Initialize the class.
        """
        gdal.UseExceptions()
        self.rasterPath: str | None = None
        self.layerName: str | None = None

        if rasterPath is not None:
            self.updateRasterInfo(rasterPath)
        if layer is not None:
            self.updateLayerInfo(layer)
    
    @staticmethod
    @contextmanager
    def gdalDatasets(path: str, close: bool = True) -> 'Generator[gdal.Dataset, None, None]':
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
    
    @staticmethod
    @contextmanager
    def orgDatasets(path: str, openType: int = 0, close: bool = True) -> 'Generator[gdal.Dataset, None, None]':
        ds = ogr.Open(path, openType) # 0 is read only, 1 is writable
        try:
            if not isinstance(ds, gdal.Dataset):
                raise RuntimeError("Failed to open layer dataset: {}".format(path))
            yield ds
        except:
            close = True
            raise RuntimeError
        finally:
            if close:
                ds.Destroy()
                gc.collect()

    def updateLayerInfo(self, layer: str | tuple[str, str]) -> None:
        layerDs = False
        try:
            if type(layer) is str:
                self.layerPath = layer
                with self.orgDatasets(self.layerPath, close=False) as layerDs:
                    layer = layerDs.GetLayer(0)
                    if not isinstance(layer, ogr.Layer):
                        raise RuntimeError("Failed to get layer from layer dataset.")
                    self.layerName = layer.GetName()
            else:
                self.layerPath, layerName = layer
                with self.orgDatasets(self.layerPath, close=False) as layerDs:
                    layer = layerDs.GetLayerByName(layerName)
                    if not isinstance(layer, ogr.Layer):
                        raise RuntimeError("Failed to get layer \'{}\' from layer dataset.".format(layerName))
                    self.layerName = layerName
            
            self.layerRef = layer.GetSpatialRef()
            if not isinstance(self.layerRef, osr.SpatialReference):
                raise RuntimeError("Failed to get spatial reference from layer dataset.")
        
        finally:
            if layerDs:
                layerDs.Destroy()
            gc.collect()  # Force garbage collection

        return
    
    def updateRasterInfo(self, rasterPath: str) -> None:
        self.rasterPath = rasterPath
        with self.gdalDatasets(rasterPath) as rasterDs:
            self.projection = rasterDs.GetProjection()
            self.geotrans = rasterDs.GetGeoTransform()
            self.ref = rasterDs.GetSpatialRef()

        return

# Debugging and testing
if __name__ == "__main__":
    pass