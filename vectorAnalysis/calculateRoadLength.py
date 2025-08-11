import sys, sqlite3, os
import geopandas as gpd
from tqdm import tqdm
from pyproj import Geod
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.append(".") # Set path to the roots

from function.sqlite import spatialiteConnection, FID_INDEX, modifyTable
from function.readFiles import readFiles, loadJsonRecord

class calculateRoadLength:
    
    @staticmethod
    def calculateOneGpkg(file: str) -> None:
        edges = gpd.read_file(file, layer="edges", encoding="utf-8")[["length", "geometry"]]
        geod = Geod(ellps="WGS84")
        edges["length"] = edges["geometry"].apply(geod.geometry_length)
        edges["fid"] = edges.index + 1
        
        conn = sqlite3.connect(file, factory=spatialiteConnection)
        conn.loadSpatialite()
        cursor = conn.cursor(factory=modifyTable)
        cursor.addFields("edges", ("length", "Real", None, False))
        edges[["fid", "length"]].to_sql("tempTable", conn, if_exists="replace", index=False)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        cursor.execute(
            """
            UPDATE edges
            SET length = (SELECT tempTable.length
                        FROM tempTable 
                        WHERE tempTable.fid = edges.fid)
            """
        )
        # cursor.execute(
        #     """
        #     UPDATE edges
        #     SET length = ST_Length(geom, 1)
        #     """
        # )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

        return
    
    def calculateAll(self, path: str, threadingNum: int = 1) -> None:
        gpkgs = set(readFiles(path).specificFile(suffix=["gpkg"]))
        log = os.path.join(path, "log.json")
        stature = loadJsonRecord(log, "length")
        if len(stature) != 0:
            for i in stature:
                gpkgs.discard(i)
            tqdm.write("The following gpkgss have already been processed and skipped: \n{}".format(stature))
        bar = tqdm(total = len(gpkgs), desc="Calculating road length", unit="layer")
        futures = []
        debugDict = {}
        with ProcessPoolExecutor(max_workers=threadingNum) as excutor:
            for file in gpkgs:
                future = excutor.submit(self.calculateOneGpkg, os.path.join(path,file))
                futures.append(future)
                debugDict[future] = file
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    tqdm.write("Error processing {}: {}".format(debugDict[future], e))
                else:
                    stature.append(debugDict[future])
                    bar.update(1)
        
        stature.save()

        return

# Debug
if __name__ == "__main__":
    # calculateRoadLength().calculateOneGpkg(r"test\\CHN.gpkg")
    calculateRoadLength().calculateAll(r"C:\\0_PolyU\\roadsGraph", threadingNum=os.cpu_count()) # type: ignore