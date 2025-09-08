import sys, os, zipfile
import geopandas as gpd
import pandas as pd
import numpy as np
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import override

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, loadJsonRecord
from nodeAnalysis.sumFloodingInfluence import allFloodingInfluence

class maxFloodingInfluenec(allFloodingInfluence):
    __slots__ = ["gpkgs", "gpkgPath", "rasters", "rasterRoot", "decompressRasterPath"]

    def __init__(self, gpkgPath: str, rasterRoot: str, decompressPath: str) -> None:
        self.gpkgs = readFiles(gpkgPath).specificFile(["gpkg"])
        self.gpkgPath = gpkgPath

        self.rasters = readFiles(rasterRoot).allFolder()
        self.rasterRoot = rasterRoot
        self.decompressRasterPath = decompressPath

        return

    def processOneRaster(self, gdf: gpd.GeoDataFrame, raster: str, excutor: ProcessPoolExecutor, bar: tqdm | None = None) -> np.ndarray:
        super().__init__(raster)
        if gdf.crs != self.crs:
            tqdm.write("Projecting!")
            gdf.to_crs(self.crs, inplace=True)

        results = np.empty([gdf.shape[0], 2], dtype=np.uint32) # Max 4294967295, because fid will exceed 65535
        chunk = 10000
        numChunks = (gdf.shape[0] + chunk - 1) // chunk
        gdfs = [gdf.iloc[i * chunk : (i + 1) * chunk] for i in range(numChunks)]
        
        for df in gdfs:
            futures = []
            debugDict = {}
            for index, geom in zip(df.index, df.geometry):
                future = excutor.submit(self.processByFid, index, geom)
                futures.append(future)
                debugDict[future] = index
            
            for future in as_completed(futures):
                index = debugDict[future]
                try:
                    fid, maxDays = future.result()
                except Exception as e:
                    raise RuntimeError("Error processing fid {}: {}".format(index, e))
                else:
                    results[index][0] = fid
                    results[index][1] = maxDays
                    if bar is not None:
                        bar.update(1)

        return results
    
    @override
    def calOneGpkg(self, gpkg: str, threadNum: int = 1, *agr, **agrs) -> bool:
        country = gpkg.split('.')[0]
        log = loadJsonRecord(os.path.join(self.gpkgPath, "log.json"), "Flooding_Road_By_Max_Influence", {})
        processedRaster = log.get(gpkg, [])
        if country not in self.rasters:
            tqdm.write("Do not found rasters for {}.".format(country))
            log.append({gpkg: ["Do not found rasters"]})
            log.save()
            return True
        
        # Read compressed raster files
        rasterRoot = os.path.join(self.rasterRoot, country)
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
            z.close()
        
        if len(realTif) == 0:
            return True

        rasters = [os.path.join(self.decompressRasterPath, x+".tif") for x in realTif]
        gpkgPath = os.path.join(self.gpkgPath, gpkg)
        gdf = gpd.read_file(gpkgPath, layer="edges", encoding="utf-8")
        bar = tqdm(total=len(rasters) * (1 + gdf.shape[0]), desc="Processing {}".format(gpkg), unit="roads")
        excutor = ProcessPoolExecutor(max_workers=threadNum)

        for raster in rasters:
            rasterName = os.path.basename(raster).split('.')[0].replace('-','_')
            bar.set_description("Processing {} in {}".format(rasterName, gpkg))

            result = self.processOneRaster(gdf, raster, excutor, bar)
            df = pd.DataFrame(result, columns=["fid", rasterName])
            if df[df[rasterName] != 0].shape[0] == 0:
                processedRaster.append(os.path.basename(raster)[:-4])
                log.append({gpkg: processedRaster})
                log.save()
                bar.update(1)
                continue  # Skip if no non-zero values found
            
            bar.set_description("Updating {} in {}".format(rasterName, gpkg))

            # Update data
            self.updateData(gpkgPath, df, rasterName)
            # Update log
            processedRaster.append(os.path.basename(raster)[:-4])    
            log.append({gpkg: processedRaster})
            log.save()
            bar.update(1)

        excutor.shutdown()
        bar.close()

        return True
    
    @override
    def calculateAll(self, threadNum: int = 1, *agr, **agrs) -> None:
        for gpkg in tqdm(self.gpkgs, desc="Processing all gpkgs", unit="gpkg"):
            self.calOneGpkg(gpkg, threadNum)

        return

# Debug
if __name__ == "__main__":
    maxFloodingInfluenec(r"C:\\0_PolyU\\test", r"D:\\flooding", r"D:\\floodingAll_Days").calculateAll(16) # type: ignore roadsGraph