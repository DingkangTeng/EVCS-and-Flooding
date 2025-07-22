import sys, gc, sqlite3, os
import pandas as pd
import geopandas as gpd
import numpy as np
from tqdm import tqdm
from scipy.spatial import KDTree
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(".") # Set path to the roots

from function.sqlite import spatialiteConnection, modifyTable
from function.readFiles import readFiles

class linkNodeWithPoints:
    def __init__(self) -> None:
        pass

    @staticmethod
    def updateData(path: str, df: pd.DataFrame) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # Add field
        cursor.addFields("nodes", ("EVCSNum", "Integer", 0), ("EVCSFids", "Text", None))
        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False)
        cursor.execute(
            """
            UPDATE nodes
            SET EVCSNum = (SELECT tempTable.EVCSNum
                        FROM tempTable 
                        WHERE tempTable.nodesFid = nodes.fid),
                EVCSFids = (SELECT tempTable.EVCSFids
                        FROM tempTable 
                        WHERE tempTable.nodesFid = nodes.fid)
            WHERE EVCSNum = 0
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
            dataPoint = gpd.read_file(layerPoint[0], layer=layerNode[1], encoding="utf-8")

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
        allNodes = readFiles(pathNode).specifcFile(suffix=["gpkg"])
        bar = tqdm(total = len(allNodes), desc="Running KD-Trees", unit="layer")
        futures = []
        debugDict = {}
        excutor = ThreadPoolExecutor(max_workers=MultiThread)
        for node in allNodes:
            path = os.path.join(pathNode, node)
            nodeName = node.split('.')[0]
            # Get corresponding EVCS layer
            # Data have not collected, using nanjin as example
            evcs = ("_GISAnalysis\\TestData\\test.gdb", "nanjin")
            future = excutor.submit(self.processOneLayer, (path, "nodes"), evcs)
            futures.append(future)
            debugDict[future] = nodeName
        
        for future in as_completed(futures):
            nodeName = debugDict[future]
            try:
                future.result()
                bar.update(1)
            except Exception as e:
                tqdm.write("Error in processing {}: \n{}".format(nodeName, e))

        bar.close()

        return

# Debug
if __name__ == "__main__":
    # linkNodeWithPoints().processOneLayer(("test//OSM_Nanjin_ThirdRoad.gpkg", "nodes"), ("_GISAnalysis\\TestData\\test.gdb", "nanjin"))
    linkNodeWithPoints().processAllLayers("test", MultiThread=os.cpu_count()) # type: ignore