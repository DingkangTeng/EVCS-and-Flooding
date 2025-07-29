import sys, os, threading, gc, sqlite3
import geopandas as gpd
import numpy as np
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, loadJsonRecord
from function.sqlite import spatialiteConnection, modifyTable
from raster.getMaxPixelsValues import getMaxPixelsValues

# Already use window in getMaxPixelsValues, do not need extra memory management when executing
class calculateFloodingInfluence:
    __slots__ = ["initialClass"]

    def __init__(self, floodingPath: str) -> None:
        self.initialClass = getMaxPixelsValues(rasterPath=floodingPath)

    def executeMethod(self, index: int, output: list[list], outputLock: threading.Lock) -> None | Exception:
            fid = index + 1
            try:
                result = self.initialClass.maxPixelsValuesByLayer(fid)
                if len(result) == 0:
                    maxDays = 0
                else:
                    maxDays = max(result)
                with outputLock:
                    output.append([fid, maxDays])
                return
                
            except Exception as e:
                return e
    
    def calOneGpkg(self, roadPath: str, gpkg: str, fieldName: str, multiThread: int = 1) -> bool:
            path = os.path.join(roadPath, gpkg)

            # Add field
            conn = sqlite3.connect(path)
            cursor = conn.cursor(factory=modifyTable)
            cursor.addFields("edges", (fieldName, "Integer", None)) # Add fields if not exists
            conn.commit()
            conn.close()

            # Initial gpkg data, skip the gpkg file which has been processed
            self.initialClass.updateLayerInfo((path, "edges"))
            gdf = gpd.read_file(path, layer="edges", encoding="utf-8")
            gdf = gdf[gdf[fieldName].isna()]
            if gdf.shape[0] == 0:
                gdf = None
                gc.collect()
                return True
            bar = tqdm(total=gdf.shape[0], desc="Processing country {}".format(gpkg.split('.')[0]), unit="road")
            gdf = None
            gc.collect()

            # Segment saving
            gdf = gpd.read_file(path, layer="edges", encoding="utf-8")
            gdf.drop(gdf.loc[~gdf[fieldName].isna()].index, inplace=True)
            indexsArray = np.array_split(gdf.index, max(1, gdf.shape[0] // 10000)) # Save every 10000 times
            output = []
            futures = []
            futuresToIndex = {} # Mapping future for debug
            outputLock = threading.Lock()
            success = True
            for indexs in indexsArray:
                try:
                    excutor = ThreadPoolExecutor(max_workers=multiThread)
                    for index in indexs:
                        # Update null value
                        future = excutor.submit(self.executeMethod, index, output, outputLock)
                        futures.append(future)
                        futuresToIndex[future] = index + 1
                    for future in as_completed(futures):
                        try:
                            future.result()
                            bar.update(1)
                        except Exception as e:
                            tqdm.write("Error in road with fid {}: {}".format(futuresToIndex[future], e))
                except Exception as e:
                    tqdm.write("Error in excuting {}: {}".format(gpkg, e))
                    success = False
                finally:
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
        cursor.execute(
            """
            UPDATE edges
            SET {} = (SELECT tempTable.{}
                        FROM tempTable 
                        WHERE tempTable.fid = edges.fid)
            WHERE {} is Null
            """.format(
                fieldName,
                fieldName,
                fieldName
            )
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

        return

    def calculateAll(
        self,
        roadPath: str,
        fieldName: str,
        specificeFile: list[str] = [],
        multiThread: int = 1
    ) -> None:
        
        if specificeFile == []:
            gpkgs = set(readFiles(roadPath).specifcFile(suffix=["gpkg"]))
        else:
            gpkgs = set(specificeFile)
        # Update log
        log = os.path.join(roadPath, "log.json")
        stature = loadJsonRecord.load(log, "Flooding_Road")
        assert type(stature) is list
        if len(stature) != 0:
            for i in stature:
                gpkgs.discard(i)
            tqdm.write("The following gpkgs have already been processed and skipped: \n{}".format(stature))
        
        # for gpkg in gpkgs:
        futures = []
        debugDict = {}
        thread = int(multiThread) # ** 0.5
        with ThreadPoolExecutor(max_workers=thread) as excutor:
            for gpkg in gpkgs:
                future = excutor.submit(self.calOneGpkg, roadPath, gpkg, fieldName, thread)
                debugDict[future] = gpkg
                futures.append(future)
            
            for future in as_completed(futures):
                gpkg = debugDict[future]
                try:
                    if future.result():
                        stature.append(gpkg)
                        loadJsonRecord.save(log, "Flooding_Road", stature)
                except Exception as e:
                    tqdm.write("Failed to process {}: {}".format(gpkg, e))

        return

if __name__ == "__main__":
    calculateFloodingInfluence("C:\\0_PolyU\\flooding\\SumDays.tif").calculateAll(
        "test",
        "affectDays",
        specificeFile=["OSM_Nanjin_ThirdRoad.gpkg"],
        multiThread=os.cpu_count() # type:ignore
    )