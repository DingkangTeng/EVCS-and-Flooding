import sys, sqlite3, os
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
    __slots__ = ["evcs", "EVCSNum", "EVCSFids"]

    def __init__(self, path: str | tuple[str, str], before: bool = True) -> None:
        self.EVCSNum = "EVCSNum" if before else "EVCSNum_After"
        self.EVCSFids = "EVCSFids" if before else "EVCSFids_After"

        if isinstance(path, str):
            self.evcs = gpd.read_file(path, encoding="utf-8", usecoles=["level1", "floodingValue", "geometry"])
        else:
            self.evcs = gpd.read_file(path[0], layer=path[1], encoding="utf-8")
            self.evcs = self.evcs[["level1", "floodingValue", "geometry"]]

        if not before: self.evcs = self.evcs[self.evcs["floodingValue"] == 0]

        self.evcs = self.evcs[["level1", "geometry"]]
        
        return

    @staticmethod
    def updateData(path: str, df: pd.DataFrame, evcs: tuple[str, str]) -> None:
        EVCSNum, EVCSFids = evcs
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # Add field
        cursor.addFields("nodes", (EVCSNum, "Integer", None, True), (EVCSFids, "Text", None, False))
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON nodes (fid)")
        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False, method="multi", chunksize=10922) #32766//3 (three columns)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        conn.commit()
        cursor.execute(
            f"""
            UPDATE nodes
            SET {EVCSNum} = tempTable.{EVCSNum}, {EVCSFids} = tempTable.{EVCSFids}
                FROM tempTable
                WHERE tempTable.nodesFid = nodes.fid
            """
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

        return
    
    # KD-Tree
    def processOneLayer(self, layerNode: tuple[str, str], layerPoint: str | tuple[str, str] | gpd.GeoDataFrame) -> None:
        # Read vector data
        path = layerNode[0] # For updates data into gpkg
        dataNode = gpd.read_file(path, layer=layerNode[1], encoding="utf-8")[["geometry"]]
        if isinstance(layerPoint, str):
            dataPoint = gpd.read_file(layerPoint, encoding="utf-8", usecoles="geometry")
        elif isinstance(layerPoint, tuple):
            dataPoint = gpd.read_file(layerPoint[0], layer=layerPoint[1], encoding="utf-8")[["geometry"]]
        else:
            dataPoint = layerPoint[["geometry"]]

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
        groupData = dataPoint.groupby("nearestFid")
        results = np.empty([len(groupData), 2], dtype=np.uint32)
        resultsStr = np.empty([len(groupData), 1], dtype=object)
        i = 0
        for nodefid, group in groupData:
            pointFids = group.index + 1
            results[i][0] = nodefid     #nodesFid
            results[i][1] = len(pointFids)      #  EVCSNum
            resultsStr[i][0] = ','.join(map(str, pointFids))   # EVCSFids
            i += 1
        
        df = pd.DataFrame(results, columns=["nodesFid", self.EVCSNum])
        df2 = pd.DataFrame(resultsStr, columns=[self.EVCSFids])
        self.updateData(path, pd.concat([df,df2], axis=1), (self.EVCSNum, self.EVCSFids))

        return
    
    def processAll(self, pathNode: str, MultiThread: int = 1) -> None:
        allNodes = readFiles(pathNode).specificFile(suffix=["gpkg"])
        allNodes.sort()
        bar = tqdm(total = len(allNodes), desc="Running KD-Trees", unit="layer")

        # Update log
        log = os.path.join(pathNode, "log.json")
        stature = loadJsonRecord(log, "EVCS" if self.EVCSNum == "EVCSNum" else "EVCS_after")
        
        futures = []
        debugDict = {}
        excutor = ThreadPoolExecutor(max_workers=MultiThread)
        for node in allNodes:
            if node in stature:
                bar.update(1)
                continue
            path = os.path.join(pathNode, node)
            # Get corresponding EVCS layer
            evcs = self.evcs[self.evcs["level1"] == node.split('.')[0]]
            if evcs.shape[0] == 0:
                bar.update(1)
                continue
            future = excutor.submit(self.processOneLayer, (path, "nodes"), evcs)
            futures.append(future)
            debugDict[future] = node
        
        for future in as_completed(futures):
            node = debugDict[future]
            try:
                future.result()
            except Exception as e:
                tqdm.write("Error in processing {}: {}".format(node, e))
            else:
                bar.update(1)
                stature.append(os.path.basename(node))

        bar.close()
        # Only successed sub-thread will append processed data into log list
        stature.save()

        return

# Debug
if __name__ == "__main__":
    # linkNodeWithPoints().processOneLayer(("test//OSM_Nanjin_ThirdRoad.gpkg", "nodes"), ("_GISAnalysis\\TestData\\test.gdb", "nanjin"))
    linkNodeWithPoints(("C:\\0_PolyU\\global_EVCS.gpkg", "evcs"), before=False).processAll("C:\\0_PolyU\\roadsGraph", MultiThread=8) # type: ignore