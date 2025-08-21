import sys, gc, os
import rasterio as rio
import geopandas as gpd
from osgeo import gdal, ogr, osr
from rasterio.features import shapes
from shapely.geometry import MultiPolygon, shape

sys.path.append(".") # Set path to the roots

from function.ogrFunction import orgDatasets
from function.gdalFunction import gdalDatasets

class getPixelsValues:
    __slots__ = [
        "layerPath", "layerName", "layerRef", "layerType",
        "rasterPath", "projection", "geotrans", "ref",  "rasterWidth", "rasterHeight",
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
        self.rasterHeight: int | None = None
        self.rasterWidth: int | None = None
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
            self.rasterWidth = rasterDs.RasterXSize
            self.rasterHeight = rasterDs.RasterYSize

        return
    
    def updateInfo(
        self, rasterInfo: tuple[str, str, tuple, str | osr.SpatialReference],
        layerInfo: tuple[str, str, str | osr.SpatialReference],
        additionInfo: dict = {}
    ) -> None:
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
        if additionInfo != {}:
            for key, value in additionInfo.items():
                setattr(self, key, value)

        return
    
    # def convertNoZeroRasterToVector(self) -> None:
    #     if self.rasterPath is None:
    #         raise RuntimeError("Raseter not initialized, run updateRasterInfo() or updateInfo().")
    #     with rio.open(self.rasterPath) as src:
    #         data = src.read(1)
    #         transform = src.transform
    #         crs = src.crs
    #         mask = (data != 0)

    #         polygons = []
    #         for geom, val in shapes(data, mask=mask, transform=transform, connectivity=8):
    #             polygons.append(shape(geom))

    #         if polygons:
    #             combined_geom = MultiPolygon(polygons)
    #         else:
    #             combined_geom = MultiPolygon()

    #         gdf = gpd.GeoDataFrame(geometry=[combined_geom], crs=crs.to_epsg())
    #         gdf.to_file(os.path.join(os.path.dirname(self.rasterPath), "{}.shp".format(os.path.basename(self.rasterPath).split('.')[0])), encoding="utf-8")  

    #     return

# Debugging and testing
if __name__ == "__main__":
    # a = getPixelsValues("C:\\0_PolyU\\flooding\\SumDays.tif").convertNoZeroRasterToVector()
    pass