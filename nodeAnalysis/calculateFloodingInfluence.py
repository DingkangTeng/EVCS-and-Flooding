import sys, os, threading, gc, sqlite3
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles
from function.sqlite import spatialiteConnection, modifyTable
from raster.getMaxPixelsValues import getMaxPixelsValues

# Already use window in getMaxPixelsValues, do not need extra memory management when executing
def calculateFloodingInfluence(
    roadPath: str,
    floodingPath: str,
    fieldName: str,
    specificeFile: list[str] = [],
    multiThread: int = 1
) -> None:
    
    def execute(initialClass: getMaxPixelsValues, index: int, output: list[list], outputLock: threading.Lock) -> None:
        fid = index + 1
        result = initialClass.maxPixelsValuesByLayer(fid)
        if len(result) == 0:
            maxDays = 0
        else:
            maxDays = max(result)
        with outputLock:
            output.append([fid, maxDays])

        return

    if specificeFile == []:
        gpkgs = readFiles(roadPath).specifcFile(suffix=["gpkg"])
    else:
        gpkgs = specificeFile
    
    initial = getMaxPixelsValues(rasterPath=floodingPath)
    
    # for gpkg in gpkgs:
    for gpkg in gpkgs:
        path = os.path.join(roadPath, gpkg)
        initial.updateLayerInfo((path, "edges"))
        gdf = gpd.read_file(path, layer="edges", encoding="utf-8")
        bar = tqdm(total=gdf.shape[0], desc="Processing country {}".format(gpkg.split('.')[0]), unit="road")
        futures = []
        futuresToIndex = {} # Mapping future for debug
        output = []
        outputLock = threading.Lock()
        excutor = ThreadPoolExecutor(max_workers=multiThread)
        for index in gdf.index:
            future = excutor.submit(execute, initial, index, output, outputLock)
            futures.append(future)
            futuresToIndex[future] = index + 1
        for future in as_completed(futures):
            try:
                future.result()
                bar.update(1)
            except Exception as e:
                tqdm.write("Error in road with fid {}: {}".format(futuresToIndex[future], e))
        
        df = pd.DataFrame(output, columns=["fid", fieldName])
        
        # Upadte data
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        cursor.addFields("edges", (fieldName, "Integer")) # Add fields if not exists
        ## Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False)
        cursor.execute(
            """
            UPDATE edges
            SET {} = (SELECT tempTable.{}
                        FROM tempTable 
                        WHERE tempTable.fid = edges.fid)
            """.format(
                fieldName,
                fieldName,
            )
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

    return

if __name__ == "__main__":
    import time
    start = time.perf_counter()
    calculateFloodingInfluence(
        "test",
        "C:\\0_PolyU\\flooding\\SumDays.tif",
        "affectDays",
        specificeFile=["OSM_Nanjin_ThirdRoad.gpkg"],
        multiThread=os.cpu_count() # type:ignore
    ) 
    end = time.perf_counter()
    print("Running time for example takes {} mins".format((end - start)/60))