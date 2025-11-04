import sys, sqlite3, os
import pandas as pd
import geopandas as gpd
import numpy as np
from tqdm import tqdm
from sklearn.neighbors import BallTree
# from scipy.spatial import KDTree
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any

sys.path.append(".") # Set path to the roots

from function.sqlite import spatialiteConnection, modifyTable, FID_INDEX
from function.readFiles import readFiles, loadJsonRecord

class linkNodeWithPoints:
    __slots__ = ["points", "pointsNum", "pointsFids", "colName"]

    def __init__(
        self,
        path: str | tuple[str, str],
        colName: str = "EVCS", filters: None | tuple[str, Any] = None,
        before: bool = True
    ) -> None:
        if before:
            self.pointsNum = "{}Num".format(colName)
            self.pointsFids = "{}Fids".format(colName)
        else:
            self.pointsNum =  "{}Num_After".format(colName)
            self.pointsFids ="{}Fids_After".format(colName)
        self.colName = colName

        if isinstance(path, str):
            self.points = (
                gpd.read_file(path, encoding="utf-8", usecoles=["level1", "floodingValue", "geometry"]) if path.split('.')[-1] != "parquet"
                else gpd.read_parquet(path)
            )
        else:
            self.points = gpd.read_file(path[0], layer=path[1], encoding="utf-8")
            self.points = self.points[["level1", "floodingValue", "geometry"]]

        if not before: self.points = self.points[self.points["floodingValue"] == 0]
        if filters is not None:
            self.points: gpd.GeoDataFrame = self.points[self.points[filters[0]] == filters[1]]
            self.points.drop(columns=filters[0], inplace=True)

        self.points = self.points[["level1", "geometry"]]
        
        return

    @staticmethod
    def updateData(path: str, df: pd.DataFrame, points: tuple[str, str]) -> None:
        pointsNum, pointsFids = points
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # Add field
        cursor.addFields("nodes", (pointsNum, "Integer", None, False), (pointsFids, "Text", None, False))
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON nodes (fid)")
        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False, method="multi", chunksize=10922) #32766//3 (three columns)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        conn.commit()
        cursor.execute(
            f"""
            UPDATE nodes
            SET {pointsNum} = tempTable.{pointsNum}, {pointsFids} = tempTable.{pointsFids}
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
        if nodeCRS.to_epsg() != 4326:
            dataNode = dataNode.to_crs("EPSG:4326")
        if pointCRS.to_epsg() != 4326:
            dataPoint = dataPoint.to_crs("EPSG:4326")

        # Old KD-Tree
        # # Conver data
        # node = np.array(list(zip(dataNode.geometry.x, dataNode.geometry.y)))
        # point = np.array(list(zip(dataPoint.geometry.x, dataPoint.geometry.y)))

        # # Build KD-Tree
        # tree = KDTree(node)
        # distance, indices = tree.query(point, k=1)

        # Conver data
        nodesRad = np.vstack([np.radians(dataNode.geometry.y), np.radians(dataNode.geometry.x)]).T
        pointsRad = np.vstack([np.radians(dataPoint.geometry.y), np.radians(dataPoint.geometry.x)]).T

        # Build KD-Tree
        tree = BallTree(nodesRad, metric="haversine") # More precise
        distance, indices = tree.query(pointsRad, k=1)
        distance = distance[:, 0] * 6371000

        # Calculates the resultes
        dataPoint["nearestFid"] = indices + 1
        dataPoint["distance"] = distance
        groupData = dataPoint.groupby("nearestFid")
        results = np.empty([len(groupData), 2], dtype=np.uint32)
        resultsStr = np.empty([len(groupData)], dtype=object)
        i = 0
        for nodefid, group in groupData:
            pointFids = group.index + 1
            pointDistances = group["distance"]
            results[i][0] = nodefid     #nodesFid
            results[i][1] = len(pointFids)      #  pointsNum
            resultsStr[i] = ','.join([
                f"{{{fid}: {round(dist, 2)}}}" for fid, dist in zip(pointFids, pointDistances)
            ])   # pointsFids and distance
            i += 1
        
        df = pd.DataFrame(results, columns=["nodesFid", self.pointsNum])
        df2 = pd.DataFrame(resultsStr, columns=[self.pointsFids])
        self.updateData(path, pd.concat([df,df2], axis=1), (self.pointsNum, self.pointsFids))

        return
    
    def processAll(self, pathNode: str, MultiThread: int = 1) -> None:
        allNodes = readFiles(pathNode).specificFile(suffix=["gpkg"])
        allNodes.sort()
        bar = tqdm(total = len(allNodes), desc="Running KD-Trees", unit="layer")

        # Update log
        log = os.path.join(pathNode, "log.json")
        stature = loadJsonRecord(log, "{}_after".format(self.colName) if self.pointsNum[-5:] == "After" else self.colName)
        
        futures = []
        debugDict = {}
        excutor = ProcessPoolExecutor(max_workers=MultiThread)
        for node in allNodes:
            if node in stature:
                bar.update(1)
                continue
            path = os.path.join(pathNode, node)
            # Get corresponding EVCS layer
            points = self.points[self.points["level1"] == node.split('.')[0]]
            if points.shape[0] == 0:
                bar.update(1)
                continue
            future = excutor.submit(self.processOneLayer, (path, "nodes"), points)
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
        excutor.shutdown()

        return

# Debug
if __name__ == "__main__":
    # linkNodeWithPoints().processOneLayer(("test//OSM_Nanjin_ThirdRoad.gpkg", "nodes"), ("_GISAnalysis\\TestData\\test.gdb", "nanjin"))
    # linkNodeWithPoints(("C:\\0_PolyU\\global_EVCS.gpkg", "evcs"), before=False).processAll("C:\\0_PolyU\\test", MultiThread=8)
    # linkNodeWithPoints(("C:\\0_PolyU\\global_EVCS.gpkg", "evcs")).processAll("C:\\0_PolyU\\roadsGraph", MultiThread=16)
    linkNodeWithPoints(("C:\\0_PolyU\\global_EVCS.gpkg", "evcs"), before=False).processAll("C:\\0_PolyU\\roadsGraph", MultiThread=16)
    # linkNodeWithPoints(r"C:\\0_PolyU\\globalPOI_2025\\extract_withCountry.parquet", "POI_1", ("fsq_category_ids", 1)).processAll("C:\\0_PolyU\\roadsGraph", MultiThread=16)
    # linkNodeWithPoints(r"C:\\0_PolyU\\globalPOI_2025\\extract_withCountry.parquet", "POI_2", ("fsq_category_ids", 2)).processAll("C:\\0_PolyU\\roadsGraph", MultiThread=1)
    # linkNodeWithPoints(r"C:\\0_PolyU\\globalPOI_2025\\extract_withCountry.parquet", "POI_3", ("fsq_category_ids", 3)).processAll("C:\\0_PolyU\\roadsGraph", MultiThread=1)