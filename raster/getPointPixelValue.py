import sys
import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio as rio

sys.path.append(".") # Set path to the roots

from raster.initRaster import initRaster

class getPointPixelsValue(initRaster):
    def getAllLayerValue(self) -> None:
        # Wait until all EVCS data are collected...

        return
    
    def getOneLayerValue(self, layer: str | tuple[str, str]) -> ...:
        # Check Initialize
        if self.rasterPath is None:
            raise RuntimeError("Have not initialized raster data, use updateRasterInfo().")

        if isinstance(layer, str):
            vectorData = gpd.read_file(layer)
        else:
            vectorData = gpd.read_file(layer[0], layer=layer[1])
        fid = vectorData.index.to_numpy() + 1

        # Check reference system
        if self.crs != vectorData.crs:
            vectorData.to_crs(self.crs, inplace=True)
        XCoords = vectorData.geometry.x.to_numpy().astype(np.float32)
        YCoords = vectorData.geometry.y.to_numpy().astype(np.float32)

        # Get raster data withing the layer extent
        with rio.open(self.rasterPath) as src:
            coords = np.array(list(zip(XCoords, YCoords)))
            values = np.array([val[0] for val in src.sample(coords)])
            return pd.DataFrame({"fid": fid, "values": values})

# Debug
if __name__ == "__main__":
    a = getPointPixelsValue("C:\\0_PolyU\\flooding\\SumDays.tif").getOneLayerValue(("_GISAnalysis\\TestData\\test.gdb", "nanjin"))
    a.to_csv("test\\CHN_EVCS_FLooding.csv", encoding="utf-8", index=False)