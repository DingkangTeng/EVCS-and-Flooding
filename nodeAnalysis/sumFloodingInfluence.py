import sys, os, time, gc, sqlite3
import geopandas as gpd
import numpy as np
import pandas as pd
from tqdm import tqdm
from rasterio.coords import BoundingBox
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from shapely.geometry.base import BaseGeometry

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, loadJsonRecord
from function.sqlite import spatialiteConnection, modifyTable, FID_INDEX
from raster.initRaster import initRaster
from raster.getPixelValuesByLine import getPixelValuesByLine

# Already use window in getMaxPixelsValues, do not need extra memory management when executing
class allFloodingInfluence:
    __slots__ = ["__rasterPath", "__crs", "__bounds"]

    def __init__(self, rasterPath: str) -> None:
        self.__rasterPath = rasterPath
        rasterInfo = initRaster(rasterPath)
        self.__crs = rasterInfo.crs
        self.__bounds = rasterInfo.bounds

    @staticmethod
    def processByFid(
        index: int,
        rasterPath: str,
        geom: BaseGeometry,
        bounds: BoundingBox
    ) -> tuple[int, int]:
            fid = index + 1
            result = getPixelValuesByLine(geom, rasterPath, bounds)
            if len(result) == 0:
                maxDays = 0
            elif np.isnan(result).all():
                maxDays = 0
            else:
                maxDays = np.nanmax(result)

            return fid, maxDays
    
    def calOneGpkg(self, roadPath: str, gpkg: str, fieldName: str, multiThread: int = 1) -> bool:
        # Initial gpkg data, skip the gpkg file which has been processed
        path = os.path.join(roadPath, gpkg)
        gdf = gpd.read_file(path, layer="edges", encoding="utf-8")
        if gdf.crs != self.__crs:
            gdf.to_crs(self.__crs, inplace=True)
        gdf = gdf[gdf[fieldName].isna()]
        if gdf.shape[0] == 0:
            gdf = None
            gc.collect()
            return True
        bar = tqdm(total=gdf.shape[0], desc="Processing country {}".format(gpkg.split('.')[0]), unit="road")

        # CPU calculation work, use process
        success = True
        with ProcessPoolExecutor(max_workers=multiThread) as excutor:
            output = []
            futures = []
            futuresToIndex = {} # Mapping future for debug
            for index, geom in zip(gdf.index, gdf.geometry):
                # Update null value
                future = excutor.submit(self.processByFid, index, self.__rasterPath, geom, self.__bounds)
                futures.append(future)
                futuresToIndex[future] = index + 1
            for future in as_completed(futures):
                try:
                    output.append(list(future.result()))
                    bar.update(1)
                except Exception as e:
                    tqdm.write("Error in road with fid {}: {}".format(futuresToIndex[future], e))
                    success = False
            if len(output) != 0:
                # Save parts of the results into gpkg and restart the processing automatically
                df = pd.DataFrame(output, columns=["fid", fieldName])
                self.updateData(path, df, fieldName)
    
        bar.close()
        return success

    @staticmethod
    def updateData(path: str, df: pd.DataFrame, fieldName: str, default = None) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # Add field
        cursor.addFields("edges", (fieldName, "Integer", default, False)) # Add fields if not exists
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON edges (fid)")
        conn.commit()
        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False, method="multi", chunksize=16383) #32766//2
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        conn.commit()
        conn.execute("BEGIN TRANSACTION;")
        cursor.execute(
            f"""
            UPDATE edges
            SET {fieldName} = tempTable.{fieldName}
                FROM tempTable 
                WHERE tempTable.fid = edges.fid
            """
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()
        time.sleep(1)
        del df
        gc.collect()

        return

    def calculateAll(
        self,
        roadPath: str,
        fieldName: str,
        specificeFile: list[str] = [],
        multiThread: int = 1
    ) -> None:
        
        if specificeFile == []:
            gpkgs = set(readFiles(roadPath).specificFile(suffix=["gpkg"]))
        else:
            gpkgs = set(specificeFile)
        # Update log
        log = loadJsonRecord(os.path.join(roadPath, "log.json"), "Flooding_Road")
        if len(log) != 0:
            for i in log:
                gpkgs.discard(i)
            tqdm.write("The following gpkgs have already been processed and skipped: \n{}".format(log))
        
        # for gpkg in gpkgs:
        futures = []
        debugDict = {}
        # IO work, using thread
        with ThreadPoolExecutor(max_workers=multiThread) as excutor:
            for gpkg in gpkgs:
                future = excutor.submit(self.calOneGpkg, roadPath, gpkg, fieldName, multiThread)
                debugDict[future] = gpkg
                futures.append(future)
            
            for future in as_completed(futures):
                gpkg = debugDict[future]
                try:
                    if future.result():
                        log.append(gpkg)
                except Exception as e:
                    tqdm.write("Failed to process {}: {}".format(gpkg, e))

        log.save()
        
        return

if __name__ == "__main__":
    allFloodingInfluence("D:\\flooding\\SumDays.tif").calculateAll(
        "test",
        "affectDays",
        multiThread=os.cpu_count() # type:ignore
    )

    # calculateFloodingInfluence("C:\\0_PolyU\\flooding\\SumDays.tif").calculateAll(
    #     "C:\\0_PolyU\\roadsGraph",
    #     "affectDays",
    #     specificeFile=["JPN - 副本.gpkg"],
    #     multiThread=int(os.cpu_count() ** 0.5) + 1 # type:ignore
    # )