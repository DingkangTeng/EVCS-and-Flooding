import sys, gc, os
import numpy as np
from osgeo import gdal, ogr, osr

sys.path.append(".") # Set path to the roots

from function.ogrFunction import orgDatasets
from function.gdalFunction import gdalDatasets

class getPixelsValues:
    __slots__ = [
        "layerPath", "layerName", "layerRef", "layerType",
        "rasterPath", "projection", "geotrans", "ref",
        "orgDatasets", "gdalDatasets"
    ]
    
    def __init__(self, rasterPath: str | None = None, layer: str | tuple[str, str] | None = None) -> None:
        """
        Initialize the class.
        """
        gdal.UseExceptions()
        gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")
        self.rasterPath: str | None = None
        self.layerName: str | None = None
        self.orgDatasets = orgDatasets
        self.gdalDatasets = gdalDatasets
        self.layerType = False # False means db data

        if rasterPath is not None:
            self.updateRasterInfo(rasterPath)
        if layer is not None:
            self.updateLayerInfo(layer)
        
        return

    def updateLayerInfo(self, layer: str | tuple[str, str]) -> None:
        layerDs = False
        try:
            if type(layer) is str:
                self.layerPath = layer
                self.layerType = True
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
        
        except:
            raise RuntimeError
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
    
    def updateInfo(self, rasterInfo: tuple[str, str, tuple, str | osr.SpatialReference], layerInfo: tuple[str, str, str | osr.SpatialReference]) -> None:
        self.rasterPath, self.projection, self.geotrans, ref = rasterInfo
        self.layerPath, self.layerName, layerRef = layerInfo
        if os.path.basename(self.layerPath).split('.')[-1] == "shp":
            self.layerType = False
        else:
            self.layerType = True
        if type(ref) is str:
            self.ref = osr.SpatialReference()
            self.ref.ImportFromWkt(ref)
        else:
            self.ref = ref
        if type(layerRef) is str:
            self.layerRef = osr.SpatialReference()
            self.layerRef.ImportFromWkt(layerRef)
        else:
            self.layerRef = layerRef

        return
    
    def convertNoZeroRasterToVector(self) -> None:
        if self.rasterPath is None:
            raise RuntimeError("Uninitialize raster, run updateRasterInfo() or updateInfo().")
        with self.gdalDatasets(self.rasterPath) as ds:
            band = ds.GetRasterBand(1)
            if not isinstance(band, gdal.Band):
                raise RuntimeError("Falied to read raster band.")
            band.SetNoDataValue(0)
            vectorDs = False
            outputDs = False

            # Creat vector
            driver = ogr.GetDriverByName("MEM")
            if not isinstance(driver, gdal.Driver):
                raise RuntimeError("Failed to creat memory driver.")
            vectorDs = driver.CreateDataSource("MEM")
            if not isinstance(vectorDs, gdal.Dataset):
                raise RuntimeError("Failed to creat vector data.")
            vectorLayer = vectorDs.CreateLayer("polygons", geom_type=ogr.wkbPolygon)
            if not isinstance(vectorLayer, ogr.Layer):
                raise RuntimeError("Failed to creat vector layer.")
            field = ogr.FieldDefn("value", ogr.OFTInteger)
            vectorLayer.CreateField(field)
            
            gdal.Polygonize(band, None, vectorLayer, 0, [], callback=None)

            # Save data
            outputDriver = ogr.GetDriverByName('ESRI Shapefile')
            if not isinstance(outputDriver, gdal.Driver):
                raise RuntimeError("Failed to creat output driver.")
            outputDs = outputDriver.CreateDataSource(
                os.path.join(
                    os.path.dirname(self.rasterPath),
                    "{}.shp".format(os.path.basename(self.rasterPath).split('.')[0])
                )
            )
            if not isinstance(outputDs, gdal.Dataset):
                raise RuntimeError("Failed to creat output layer.")
            outputDs.CopyLayer(vectorLayer, "polygons")

            # Save vector
            if vectorDs:
                vectorDs.FlushCache()
                vectorDs.Close()
                driver = None
            if outputDs:
                outputDs.FlushCache()
                outputDs.Close()
                outputDriver = None

        return

# Debugging and testing
if __name__ == "__main__":
    a = getPixelsValues("C:\\0_PolyU\\flooding\\SumDays.tif").convertNoZeroRasterToVector()