import sys, os, threading, gc, sqlite3
import geopandas as gpd
import numpy as np
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles
from function.sqlite import spatialiteConnection, modifyTable
from raster.getMaxPixelsValues import getMaxPixelsValues

# Already use window in getMaxPixelsValues, do not need extra memory management when executing
class calculateFloodingInfluence:
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
            gpkgs = readFiles(roadPath).specifcFile(suffix=["gpkg"])
        else:
            gpkgs = specificeFile
        
        # for gpkg in gpkgs:
        for gpkg in gpkgs:
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
            gdf.drop(gdf.loc[~gdf[fieldName].isna()].index, inplace=True)
            if gdf.shape[0] == 0:
                gdf = None
                gc.collect()
                continue
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
                finally:
                    # Save parts of the results into gpkg and restart the processing automatically
                    df = pd.DataFrame(output, columns=["fid", fieldName])
                    self.updateData(path, df, fieldName)
            
            bar.close()

        return

if __name__ == "__main__":
    calculateFloodingInfluence("C:\\0_PolyU\\flooding\\SumDays.tif").calculateAll(
        "test",
        "affectDays",
        specificeFile=["OSM_Nanjin_ThirdRoad.gpkg"],
        multiThread=os.cpu_count() * 2 # type:ignore
    )