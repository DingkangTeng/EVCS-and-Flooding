import sys, gc
import numpy as np
from osgeo import gdal, ogr, osr

sys.path.append(".") # Set path to the roots

from raster.getPixelsValues import getPixelsValues

class getMaxPixelsValues(getPixelsValues):
    def maxPixelsValuesByLayer(self, fid: int, band: int=1) -> list:
        with self.orgDatasets(self.layerPath) as layerDs:
            result = self.maxPixelsValuesByFid(layerDs, fid, band)
            if not isinstance(result, Exception):
                return result
            else:
                raise RuntimeError(result)

    def maxPixelsValuesByFid(self, layerDs: gdal.Dataset, fid: int, band: int=1) -> list | Exception:
        """
        Get pixel values along one layer with fid in a raster.
        """
        # Check Initialize
        if self.rasterPath is None:
            raise RuntimeError("Have not initialized raster data, use updateRasterInfo().")
        driver = gdal.GetDriverByName("MEM")
        if not isinstance(driver, gdal.Driver):
            raise RuntimeError("Failed to creat memory driver.")
        memDs = False
        maskDs = False
        outDs = False
        # Query the layer by fid
        sql = "SELECT * FROM {} WHERE fid = {}".format(self.layerName, fid)
        querylayer = layerDs.ExecuteSQL(sql)
        isQuery = True
        ## Check if the query was successful
        if not isinstance(querylayer, ogr.Layer):
            layerDs.ReleaseResultSet(querylayer)  # Release Rousce
            raise RuntimeError("Failed to execute SQL query: {}".format(sql))
        elif querylayer is None:
            layerDs.ReleaseResultSet(querylayer)  # Release Rousce
            raise RuntimeError("Failed to execute SQL query: {}".format(sql))
        elif querylayer.GetFeatureCount() == 0:
            layerDs.ReleaseResultSet(querylayer)  # Release Rousce
            raise RuntimeError("No result in SQL query: {}".format(sql))
        
        try:
            # Coordinates projection
            assert isinstance(self.layerRef, osr.SpatialReference)
            if not self.layerRef.IsSame(self.ref):
                transform = osr.CoordinateTransformation(self.layerRef, self.ref)
                # points = [
                #     (XMin, YMin),
                #     (XMin, YMax),
                #     (XMax, YMin),
                #     (XMax, YMax)
                # ]
                # transformed = [transform.TransformPoint(*p) for p in points]
                # coords = np.array(transformed)[:, :2]
                # XMin, YMin = coords.min(axis=0)
                # XMax, YMax = coords.max(axis=0)
                outDs = driver.CreateDataSource("memData")
                if not isinstance(outDs, gdal.Dataset):
                    raise RuntimeError("Failed to creat memeory dataset for projection.")
                outLayer = outDs.CreateLayer('memLayer', srs=self.ref, geom_type=querylayer.GetGeomType())
                if not isinstance(outLayer, ogr.Layer):
                    raise RuntimeError("Faile to creat memeory layer for projection.")
                for feature in querylayer:
                    if not isinstance(feature, ogr.Feature):
                        continue
                    geom = feature.GetGeometryRef()
                    if not isinstance(geom, ogr.Geometry):
                        continue
                    geom.Transform(transform)
                    outFeature = ogr.Feature(outLayer.GetLayerDefn())
                    outFeature.SetGeometry(geom)
                    outLayer.CreateFeature(outFeature)
                    outFeature = None
                    gc.collect()
                layerDs.ReleaseResultSet(querylayer)
                querylayer = outLayer
                isQuery = False
            
            XMin, XMax, YMin, YMax = querylayer.GetExtent()
            # Aviod the problem that the vector are too short to get a rectangle
            if XMin == XMax:
                XMin -= 0.0000001
                XMax += 0.0000001
            if YMin == YMax:
                YMin -= 0.0000001
                YMax += 0.0000001

            # Get raster data withing the layer extent
            warpOptions = gdal.WarpOptions(
                format="MEM", # Use in-memory dataset
                outputBounds=[XMin, YMin, XMax, YMax], # Set the extent to the layer
                cropToCutline=True, # Crop the raster to the extent of the mask
                dstNodata=0,
                multithread=True,
            )
            try:
                memDs = gdal.Warp('', self.rasterPath, options=warpOptions)
            except:
                raise RuntimeError("Failed to excute gdal.Warp()")
            if not isinstance(memDs, gdal.Dataset):
                raise RuntimeError("Failed to warp raster with the provided options.")
            rasterArray = memDs.ReadAsArray()
            rasterArray = np.ma.masked_equal(rasterArray, 0)
            if rasterArray is None:
                raise RuntimeError("Failed to read raster band as array.")
            
            # Creat layer mask
            cols = memDs.RasterXSize
            rows = memDs.RasterYSize
            if not isinstance(driver, gdal.Driver):
                raise RuntimeError("Memory driver not available.")
            maskDs = driver.Create('', cols, rows, 1, gdal.GDT_Byte)
            if not isinstance(maskDs, gdal.Dataset):
                raise RuntimeError("Failed to create memory dataset for mask.")
            maskDs.SetGeoTransform(memDs.GetGeoTransform())
            maskDs.SetProjection(memDs.GetProjection())
            maskBand = maskDs.GetRasterBand(band)
            if not isinstance(maskBand, gdal.Band):
                raise RuntimeError("Failed to get band from mask dataset.")
            maskBand.SetNoDataValue(0)
            maskBand.Fill(0)  # Initialize mask with zeros
            
            # Create mask array for the layer
            err = gdal.RasterizeLayer(maskDs, [1], querylayer, burn_values=[1], options=["ALL_TOUCHED=FALSE"])
            if err != gdal.CE_None:
                raise RuntimeError("Rasterization failed with error code: {}".format(err))
            maskBand = maskDs.GetRasterBand(band)
            if not isinstance(maskBand, gdal.Band):
                raise RuntimeError("Failed to get band from rasterized dataset.")
            maskArray = maskBand.ReadAsArray()
            
            # Apply the mask to the raster
            maskedArray = np.where(maskArray == 1, rasterArray, 0)

            # # If you want to plot, you can use matplotlib:
            # import matplotlib.pyplot as plt
            # plt.imshow(maskedArray)
            # plt.show()

            # Filter results
            result = maskedArray.reshape(-1)
            result = result[result != 0]
            
            return result.tolist()  # Return the maximum pixel value along the layer
        
        except Exception as e:
            return e
        finally:
            # Release Rousce
            if memDs:
                memDs.FlushCache()
                memDs.Destroy()
            if maskDs:
                maskDs.FlushCache()
                maskDs.Destroy()
            if outDs:
                assert isinstance(outDs, gdal.Dataset)
                outDs.FlushCache()
                outDs.Destroy()
            if isQuery:
                layerDs.ReleaseResultSet(querylayer)
            gc.collect()


# Debugging and testing
if __name__ == "__main__":
    # Example usage
    import geopandas as gpd
    import time
    start = time.perf_counter()
    a = getMaxPixelsValues("C:\\0_PolyU\\flooding\\SumDays.tif")
    for i in ["test\\OSM_Nanjin_ThirdRoad.gpkg"]:
        a.updateLayerInfo((i, "edges"))
        # layers = gpd.read_file(i, layer="edges", encoding="utf-8")
        # for index in layers.index:
        #     values = a.maxPixelsValuesByLayer(index + 1) # Index + 1 is fid
        values = a.maxPixelsValuesByLayer(13921) # Index + 1 is fid
        print(set(values)) #1414 support value: set(0,2,3,4,5,7,8); 16897: set(1, 21); 14499: set()
    end = time.perf_counter()
    print("Running time for example takes {} mins".format((end - start)/60))