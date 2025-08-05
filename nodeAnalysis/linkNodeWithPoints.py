import sys, sqlite3, os, threading
import pandas as pd
import geopandas as gpd
import numpy as np
from tqdm import tqdm
from scipy.spatial import KDTree
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(".") # Set path to the roots

from function.sqlite import spatialiteConnection, modifyTable, FID_INDEX
from function.readFiles import readFiles, loadJsonRecord

class linkNodeWithPoints:
    def __init__(self) -> None:
        pass

    @staticmethod
    def updateData(path: str, df: pd.DataFrame) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # Add field
        cursor.addFields("nodes", ("EVCSNum", "Integer", None, True), ("EVCSFids", "Text", None, False))
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON nodes (fid)")
        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        conn.commit()
        cursor.execute(
            """
            UPDATE nodes
            SET EVCSNum = (SELECT tempTable.EVCSNum
                        FROM tempTable 
                        WHERE tempTable.nodesFid = nodes.fid),
                EVCSFids = (SELECT tempTable.EVCSFids
                        FROM tempTable 
                        WHERE tempTable.nodesFid = nodes.fid)
            WHERE EVCSNum is NULL
            """
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

        return
    
    # KD-Tree
    def processOneLayer(self, layerNode: tuple[str, str], layerPoint: str | tuple[str, str]) -> None:
        # Read vector data
        path = layerNode[0] # For updates data into gpkg
        dataNode = gpd.read_file(path, layer=layerNode[1], encoding="utf-8")
        if type(layerPoint) is str:
            dataPoint = gpd.read_file(layerPoint, encoding="utf-8")
        else:
            dataPoint = gpd.read_file(layerPoint[0], layer=layerPoint[1], encoding="utf-8")

        # Change CRS
        nodeCRS = dataNode.crs
        pointCRS = dataPoint.crs
        if nodeCRS is None:
            raise RuntimeError("{} do not have reference system.".format(layerNode))
        elif pointCRS is None:
            raise RuntimeError("{} do not have reference system.".format(layerPoint))
        elif nodeCRS != pointCRS:
            dataPoint.to_crs(nodeCRS, inplace=True)

        # Conver data
        node = np.array(list(zip(dataNode.geometry.x, dataNode.geometry.y)))
        point = np.array(list(zip(dataPoint.geometry.x, dataPoint.geometry.y)))

        # Build KD-Tree
        tree = KDTree(node)
        _, indices = tree.query(point, k=1)

        # Calculates the resultes
        dataPoint["nearestFid"] = indices + 1
        results = []
        for nodefid, group in dataPoint.groupby("nearestFid"):
            pointFids = np.array(group.index) + 1
            count = len(pointFids)
            results.append({
                "nodesFid": nodefid,
                "EVCSNum": count,
                "EVCSFids": ','.join(map(str, pointFids))
            })
        
        self.updateData(path, pd.DataFrame(results))

        return
    
    def processAllLayers(self, pathNode: str, MultiThread: int = 1) -> None:
        def excute(layerNode: tuple[str, str], layerPoint: str | tuple[str, str], stature: tuple[list, threading.Lock, tqdm]) -> None:
            l, lock, bar = stature
            try:
                self.processOneLayer(layerNode, layerPoint)
            except Exception as e:
                raise RuntimeError(e)
            else:
                with lock:
                    bar.update(1)
                    l.append(os.path.basename(layerNode[0]))

        allNodes = set(readFiles(pathNode).specificFile(suffix=["gpkg"]))
        # Update log
        log = os.path.join(pathNode, "log.json")
        stature = loadJsonRecord.load(log, "EVCS")
        assert type(stature) is list
        if len(stature) != 0:
            for i in stature:
                allNodes.discard(i)
            tqdm.write("The following gpkgs have already been processed and skipped: \n{}".format(stature))
        bar = tqdm(total = len(allNodes), desc="Running KD-Trees", unit="layer")
        futures = []
        debugDict = {}
        excutor = ThreadPoolExecutor(max_workers=MultiThread)
        lock = threading.Lock()
        for node in allNodes:
            path = os.path.join(pathNode, node)
            nodeName = node.split('.')[0]
            # Get corresponding EVCS layer
            # Data have not collected, using nanjin as example
            '''
            !!!!
            '''
            evcs = ("_GISAnalysis\\TestData\\test.gdb", "nanjin")
            future = excutor.submit(excute, (path, "nodes"), evcs, (stature, lock, bar))
            futures.append(future)
            debugDict[future] = nodeName
        
        for future in as_completed(futures):
            nodeName = debugDict[future]
            try:
                future.result()
            except Exception as e:
                tqdm.write("Error in processing {}: {}".format(nodeName, e))

        bar.close()
        # Only successed sub-thread will append processed data into log list
        loadJsonRecord.save(log, "EVCS", stature)

        return

# Debug
if __name__ == "__main__":
    # linkNodeWithPoints().processOneLayer(("test//OSM_Nanjin_ThirdRoad.gpkg", "nodes"), ("_GISAnalysis\\TestData\\test.gdb", "nanjin"))
    linkNodeWithPoints().processAllLayers("test", MultiThread=os.cpu_count()) # type: ignore