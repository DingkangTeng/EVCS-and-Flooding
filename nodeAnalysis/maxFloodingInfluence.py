# 路网文件-》遍历每个洪水event，算出每个event的影响时间，保存列-》确定最大影响event，大概就是这么个思路
import sys, os, sqlite3, zipfile
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from osgeo import osr

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, loadJsonRecord
from raster.getMaxPixelsValues import getMaxPixelsValues
from function.sqlite import spatialiteConnection, modifyTable, FID_INDEX
from nodeAnalysis.allFloodingInfluence import allFloodingInfluence

class maxFloodingInfluenec(allFloodingInfluence):
    __slots__ = ["gpkgs", "gpkgPath", "rasters", "rasterPath", "decompressRasterPath"]

    def __init__(self, gpkgPath: str, rasterPath: str, decompressPath: str) -> None:
        self.gpkgs = readFiles(gpkgPath).specificFile(["gpkg"])
        self.gpkgPath = gpkgPath

        self.rasters = readFiles(rasterPath).allFolder()
        self.rasterPath = rasterPath
        self.decompressRasterPath = decompressPath

        return

    def processOneRaster(self, gpkg: tuple[list, str], raster: str, threadNum: int = 1, bar: tqdm | None = None) -> list[list[int]]:
        results = []
        indexs, gpkgPath = gpkg
        initial = getMaxPixelsValues(rasterPath=raster, layer=(gpkgPath, "edges"))
        if type(initial.rasterPath) is str and type(initial.projection) is str and type(initial.geotrans) is tuple and isinstance(initial.ref, osr.SpatialReference):
            rasterInfo = (initial.rasterPath, initial.projection, initial.geotrans, initial.ref.ExportToWkt())
            additionInfo = {"rasterWidth": initial.rasterWidth, "rasterHeight": initial.rasterWidth}
        else:
            raise RuntimeError("Failed to load raster data.")
        if type(initial.layerPath) is str and type(initial.layerName) is str and isinstance(initial.layerRef, osr.SpatialReference):
                layerInfo = (initial.layerPath, initial.layerName, initial.layerRef.ExportToWkt())
        else:
            raise RuntimeError("Failed to load layer {}".format(gpkgPath))

        futures = []
        debugDict = {}
        with ProcessPoolExecutor(max_workers=threadNum) as excutor:
            if bar is not None:
                bar.set_description("Submitting tasks for {} in {}".format(os.path.basename(raster), os.path.basename(gpkgPath)))
            for index in indexs:
                future = excutor.submit(self.processByFid, index, rasterInfo, layerInfo, additionInfo)
                futures.append(future)
                debugDict[future] = index
                if bar is not None:
                    bar.update(1)
            
            if bar is not None:
                bar.set_description("Processing tasks for {} in {}".format(os.path.basename(raster), os.path.basename(gpkgPath)))
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    tqdm.write("Error processing fid {}: {}".format(debugDict[future], e))
                else:
                    results.append(result)
                    if bar is not None:
                        bar.update(1)

        return results
    
    def porcessOneGpkg(self, gpkg: str, threadNum: int = 1) -> None:
        country = gpkg.split('.')[0]
        log = loadJsonRecord(os.path.join(self.gpkgPath, "log.json"), "Flooding_Road_By_Max_Influence", {})
        processedRaster = log.get(gpkg, [])
        if country not in self.rasters:
            tqdm.write("Do not found rasters for {}.".format(country))
            return
        
        # Read compressed raster files
        rasterRoot = os.path.join(self.rasterPath, country)
        rasterZips = readFiles(rasterRoot).specificFile(["zip"])
        realTif = []
        for file in rasterZips:
            zipPath = os.path.join(rasterRoot, file)
            z = zipfile.ZipFile(zipPath, 'r')
            for tif in z.namelist():
                if tif.split('.')[-1] != "tif":
                    continue
                elif tif not in processedRaster:
                    realTif.append(tif)
                else:
                    tqdm.write("Raster {} has already been processed, skipped.".format(tif))
            z.close()
        
        if len(realTif) == 0:
            tqdm.write("No new rasters found for {}.".format(gpkg))
            return

        rasters = [os.path.join(self.decompressRasterPath, x+".tif") for x in realTif]
        gpkgPath = os.path.join(self.gpkgPath, gpkg)
        gdf = gpd.read_file(gpkgPath, layer="edges", encoding="utf-8")
        bar = tqdm(total=len(rasters) * (1 + gdf.shape[0] * 2), desc="Processing {}".format(gpkg), unit="raster")
        
        for raster in rasters:
            rasterName = os.path.basename(raster).split('.')[0].replace('-','_')
            bar.set_description("Processing {} in {}".format(rasterName, gpkg))


            result = self.processOneRaster((gdf.index.to_list(), gpkgPath), raster, threadNum, bar)
            df = pd.DataFrame(result, columns=["fid", rasterName])
            if df[df[rasterName] != 0].shape[0] == 0:
                tqdm.write("No non-zero values found in {}".format(rasterName))
                processedRaster.append(os.path.basename(raster)[:-4])
                log.append({gpkg: processedRaster})
                log.save()
                bar.update(1)
                continue  # Skip if no non-zero values found
            
            bar.set_description("Updating {} in {}".format(rasterName, gpkg))

            # Add field
            conn = sqlite3.connect(gpkgPath)
            cursor = conn.cursor(factory=modifyTable)
            cursor.addFields("edges", (rasterName, "Integer", None, True)) # Add fields if not exists
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON edges (fid)")
            conn.commit()
            conn.close()
            # Update data
            self.updateData(gpkgPath, df, rasterName)
            # Update log
            processedRaster.append(os.path.basename(raster)[:-4])    
            log.append({gpkg: processedRaster})
            log.save()
            bar.update(1)

        bar.close()

        return
    
    def processAll(self, threadNum: int = 1) -> None:
        for gpkg in self.gpkgs:
            self.porcessOneGpkg(gpkg, threadNum)

        return

# Debug
if __name__ == "__main__":
    maxFloodingInfluenec(r"C:\\0_PolyU\\roadsGraph", r"C:\\0_PolyU\\flooding", r"C:\\0_PolyU\\floodingAll_Days").porcessOneGpkg("BRA.gpkg", int(os.cpu_count())) # type: ignore
    # maxFloodingInfluenec(r"C:\\0_PolyU\\roadsGraph", r"C:\\0_PolyU\\flooding", r"C:\\0_PolyU\\floodingAll_Days").processAll(threadNum=os.cpu_count()) # type: ignore