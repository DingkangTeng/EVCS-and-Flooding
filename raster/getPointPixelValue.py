import sys
import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio as rio
from osgeo import osr
from pyproj import Transformer

sys.path.append(".") # Set path to the roots

from raster.getPixelsValues import getPixelsValues
from function.gdalFunction import getRasterByRectangleBoundary

class getPointPixelsValue(getPixelsValues):
    def getAllLayerValue(self):

        return
    
    def getOneLayerValue(self, layer: str | tuple[str, str], multiLayerLock: bool = False):
        # Check Initialize
        if self.rasterPath is None:
            raise RuntimeError("Have not initialized raster data, use updateRasterInfo().")
        self.updateLayerInfo(layer)
        vectorData = gpd.read_file(self.layerPath, layer=self.layerName)
        XCoords = vectorData.geometry.x.to_numpy().astype(np.float32)
        YCoords = vectorData.geometry.y.to_numpy().astype(np.float32)
        fid = vectorData.index.to_numpy() + 1

        # Check reference system
        assert isinstance(self.ref, osr.SpatialReference)
        assert isinstance(self.layerRef, osr.SpatialReference)
        if not self.ref.IsSame(self.layerRef):
            transformer = Transformer.from_crs(
                self.layerRef.ExportToProj4(), 
                self.ref.ExportToProj4(),
                always_xy=True
            )
            XCoords, YCoords = transformer.transform(XCoords, YCoords)

        # Get raster data withing the layer extent
        if multiLayerLock:
            pass
        else:
            with rio.open(self.rasterPath) as src:
                coords = np.array(list(zip(XCoords, YCoords)))
                values = np.array([val[0] for val in src.sample(coords)])
                return pd.DataFrame({"fid": fid, "values": values})

# Debug
if __name__ == "__main__":
    getPointPixelsValue("C:\\0_PolyU\\flooding\\SumDays.tif").getOneLayerValue(("_GISAnalysis\\TestData\\test.gdb", "nanjin"))