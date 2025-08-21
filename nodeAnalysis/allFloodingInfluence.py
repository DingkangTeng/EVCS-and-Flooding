import sys, os, time, gc, sqlite3
import geopandas as gpd
import numpy as np
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from osgeo import osr

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, loadJsonRecord
from function.sqlite import spatialiteConnection, modifyTable, FID_INDEX
from raster.getMaxPixelsValues import getMaxPixelsValues

# Already use window in getMaxPixelsValues, do not need extra memory management when executing
class allFloodingInfluence:
    __slots__ = ["initial", "rasterInfo"]

    def __init__(self, floodingPath: str) -> None:
        self.initial = getMaxPixelsValues(rasterPath=floodingPath)
        if type(self.initial.rasterPath) is str and type(self.initial.projection) is str and type(self.initial.geotrans) is tuple and isinstance(self.initial.ref, osr.SpatialReference):
            self.rasterInfo = (self.initial.rasterPath, self.initial.projection, self.initial.geotrans, self.initial.ref.ExportToWkt())
        else:
            raise RuntimeError("Failed to load raster data.")

    @staticmethod
    def processByFid(
        index: int,
        rasterInfo: tuple[str, str, tuple, str],
        layerInfo: tuple[str, str, str],
        additionInfo: dict = {}
    ) -> list[int]:
            fid = index + 1
            process = getMaxPixelsValues()
            process.updateInfo(rasterInfo, layerInfo, additionInfo)
            result = process.maxPixelsValuesByLayer(fid)
            if len(result) == 0:
                maxDays = 0
            else:
                maxDays = max(result)

            return [fid, maxDays]
    
    def calOneGpkg(self, roadPath: str, gpkg: str, fieldName: str, multiThread: int = 1) -> bool:
            path = os.path.join(roadPath, gpkg)

            # Add field
            conn = sqlite3.connect(path)
            cursor = conn.cursor(factory=modifyTable)
            cursor.addFields("edges", (fieldName, "Integer", None, True)) # Add fields if not exists
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON edges (fid)")
            conn.commit()
            conn.close()

            # Initial gpkg data, skip the gpkg file which has been processed
            self.initial.updateLayerInfo((path, "edges"))
            if type(self.initial.layerPath) is str and type(self.initial.layerName) is str and isinstance(self.initial.layerRef, osr.SpatialReference):
                layerInfo = (self.initial.layerPath, self.initial.layerName, self.initial.layerRef.ExportToWkt())
            else:
                raise RuntimeError("Failed to load layer {}".format(gpkg))
            gdf = gpd.read_file(path, layer="edges", encoding="utf-8")
            gdf = gdf[gdf[fieldName].isna()]
            if gdf.shape[0] == 0:
                gdf = None
                gc.collect()
                return True
            bar = tqdm(total=gdf.shape[0], desc="Processing country {}".format(gpkg.split('.')[0]), unit="road")

            # 能不能按栅格非0的部分初筛一下，把未在栅格区间的直接赋值0？

            # Segment saving
            indexsArray = np.array_split(gdf.index, max(1, gdf.shape[0] // 10000)) # Save every 10000 times
            success = True
            # CPU calculation work, use process
            with ProcessPoolExecutor(max_workers=multiThread) as excutor:
                for indexs in indexsArray:
                    output = []
                    futures = []
                    futuresToIndex = {} # Mapping future for debug
                    for index in indexs:
                        # Update null value
                        future = excutor.submit(self.processByFid, index, self.rasterInfo, layerInfo)
                        futures.append(future)
                        futuresToIndex[future] = index + 1
                    for future in as_completed(futures):
                        try:
                            output.append(future.result())
                            bar.update(1)
                        except Exception as e:
                            tqdm.write("Error in road with fid {}: {}".format(futuresToIndex[future], e))
                            success = False
                    if len(output) != 0:
                        # Save parts of the results into gpkg and restart the processing automatically
                        df = pd.DataFrame(output, columns=["fid", fieldName])
                        self.updateData(path, df, fieldName)
            
            bar.close()
            if success:
                return True
            else:
                return False
    
    @staticmethod
    def updateData(path: str, df: pd.DataFrame, fieldName: str) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor()
        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        conn.commit()
        cursor.execute(
            f"""
            UPDATE edges
            SET {fieldName} = (SELECT tempTable.{fieldName}
                        FROM tempTable 
                        WHERE tempTable.fid = edges.fid)
            WHERE edges.fid IN (SELECT fid FROM tempTable)
                AND {fieldName} is Null
            """
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()
        time.sleep(1)

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
        log = os.path.join(roadPath, "log.json")
        stature = loadJsonRecord(log, "Flooding_Road")
        if len(stature) != 0:
            for i in stature:
                gpkgs.discard(i)
            tqdm.write("The following gpkgs have already been processed and skipped: \n{}".format(stature))
        
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
                        stature.append(gpkg)
                except Exception as e:
                    tqdm.write("Failed to process {}: {}".format(gpkg, e))

        stature.save()
        
        return

if __name__ == "__main__":
    allFloodingInfluence("C:\\0_PolyU\\flooding\\SumDays.tif").calculateAll(
        "test",
        "affectDays",
        specificeFile=["CHN.gpkg"],
        multiThread=os.cpu_count() # type:ignore
    )

    # calculateFloodingInfluence("C:\\0_PolyU\\flooding\\SumDays.tif").calculateAll(
    #     "C:\\0_PolyU\\roadsGraph",
    #     "affectDays",
    #     specificeFile=["JPN - 副本.gpkg"],
    #     multiThread=int(os.cpu_count() ** 0.5) + 1 # type:ignore
    # )