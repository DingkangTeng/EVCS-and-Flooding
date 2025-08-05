import sys, sqlite3, os
import osmnx as ox
import networkx as nx
import geopandas as gpd
import pandas as pd
import numpy as np
from tqdm import tqdm
from concurrent.futures import as_completed, ProcessPoolExecutor, ThreadPoolExecutor

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, mkdir
from function.sqlite import spatialiteConnection, modifyTable, FID_INDEX

class M2SFCA:
    __slots__ = []
    NODES_ATTR = [
        "geometry", "osmid", "EVCSNum", "EVCSFids", "allPopulation"
    ]
    EDGES_ATTR = [
        "geometry", "highway", "length", "affectDays"
    ]

    @staticmethod
    def decayFunc(distance, d0, func: str) -> float:
        """Gaussian decay function"""
        if func == "Gaussian":
            return np.exp(-0.5 * (distance / d0) ** 2)
        else:
            raise RuntimeError(" \
                Unexceptional decay function. Available function: \n \
                Gaussian decay function; \
            ")
    
    @staticmethod
    def updateData(path: str, df: pd.DataFrame, fieldName: str) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        cursor.addFields("nodes", (fieldName, "Real", None, False))
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON nodes (fid)")
        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        conn.commit()
        cursor.execute(
            f"""
            UPDATE nodes
            SET {fieldName} = (SELECT tempTable.{fieldName}
                        FROM tempTable 
                        WHERE tempTable.fid = nodes.fid)
            """
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

        return
    
    def getGraph(self, file: str, filter: str = '') -> tuple[nx.MultiDiGraph, list[int]]:
        nodes = gpd.read_file(file, layer="nodes", encoding="utf-8")[self.NODES_ATTR]
        edges = gpd.read_file(file, layer="edges", encoding="utf-8").set_index(['u', 'v', "key"])[self.EDGES_ATTR]

        if "afterFlooding" in filter.split('_') :
            # EVCS use Nanjing's as example, modify is later
            '''
            !!!!
            '''
            evcs = pd.read_csv("test\\CHN_EVCS_Flooding.csv", encoding="utf-8")
            evcs = set(evcs.loc[evcs["values"] != 0, "fid"].astype(str).to_list())
            # One analysis way is to delete affected edges directly, the other is use affected days/times as weight
            edges = edges.loc[edges["affectDays"] == 0]
            # drop all affected evcs for first try, maby change to a complex algorithm to calculates the weights
            for i in nodes.index:
                EVCSFids = nodes.loc[i, "EVCSFids"]
                if EVCSFids is not None and isinstance(EVCSFids, str):
                    fids = set(EVCSFids.split(','))
                    if (fids & evcs) != set(): # EVCSFid have intersection with affected EVCS
                        nodes.loc[i, "EVCSNum"] = len(fids - evcs)

        return ox.convert.graph_from_gdfs(nodes, edges), nodes.index.to_list()
    
    def demandDijkstra(self, G: nx.MultiDiGraph, node: int, d0: float, decayFunc: str, demandAttr: str, EVCSnum: float) -> float:
        totalWeightedDemand = 0
        # Calculate the accessable demand points from supply node within distance d0
        try:
            paths = nx.single_source_dijkstra_path_length(G, node, cutoff=d0, weight="length")
            for i, distance in paths.items():
                demandValue = G.nodes[i].get(demandAttr, 0)
                weightVal = self.decayFunc(distance, d0, decayFunc)
                totalWeightedDemand += demandValue * weightVal
        # No access path
        except nx.NetworkXNoPath:
            pass

        if totalWeightedDemand > 0:
            return EVCSnum / totalWeightedDemand
        else:
            return 0
    
    def supplyDijKstra(self, G: nx.MultiDiGraph, node: int, d0: float, decayFunc: str, supplyNodes: list[int], R: dict[int, float]) -> float:
        totalWeightedAccess = 0
        # Calculate the accessable supply points from demand node within distance d0
        try:
            reversePaths = nx.single_source_dijkstra_path_length(G.reverse(copy=False), node, cutoff=d0, weight="length")
            for i, distance in reversePaths.items():
                # Only consider supply nodes
                if i in supplyNodes:
                    weightVal = self.decayFunc(distance, d0, decayFunc)
                    totalWeightedAccess += R.get(i, 0) * weightVal
        # No access path
        except nx.NetworkXNoPath:
            pass
            
        return totalWeightedAccess
    
    def calOneLayer(self, file: str, d0: float, decayFunc: str, filter: str = '', maxThreads: int = 1) -> None:
        """
        filter:
        afterFlooding: calculates all population after flooding
        ...
        """
        G, nodesIndex = self.getGraph(file, filter)
        fileName = os.path.basename(file)
        bar = tqdm(total=len(nodesIndex) * 2 + 6, desc="Calcunating Demand of {}".format(fileName))

        if filter == "afterFlooding":
            demandAttr = "allPopulation"
            fieldName = "afterFlooding"
        else:
            demandAttr = "allPopulation"
            fieldName = "noFlooding"
        
        # Demand point
        R = {}
        futures = []
        dbugDict = {}
        with ThreadPoolExecutor(max_workers=maxThreads) as excutor:
            for node in nodesIndex:
                EVCSnum = G.nodes[node].get("EVCSNum", None)
                if EVCSnum is not None:
                    future = excutor.submit(self.demandDijkstra, G, node, d0, decayFunc, demandAttr, EVCSnum)
                    futures.append(future)
                    dbugDict[future] = node
                else:
                    bar.update(1)
            for future in as_completed(futures):
                node = dbugDict[future]
                try:
                    result = future.result()
                    R[node] = result
                    bar.update(1)
                except Exception as e:
                    raise RuntimeError("Failed to process {}: {}".format(node, e))
        
        # Accessibility
        supplyNodes = list(R.keys())
        A = {}
        futures = []
        dbugDict = {}
        bar.set_description("Calcunating Supply of {}".format(fileName))
        with ThreadPoolExecutor(max_workers=maxThreads) as excutor:
            for node in nodesIndex:
                demandValue = G.nodes[node].get(demandAttr, 0)
                if demandValue != 0:
                    future = excutor.submit(self.supplyDijKstra, G, node, d0, decayFunc, supplyNodes, R)
                    futures.append(future)
                    dbugDict[future] = node
                else:
                    bar.update(1)
            for future in as_completed(futures):
                node = dbugDict[future]
                try:
                    result = future.result()
                    A[node] = result
                    bar.update(1)
                except Exception as e:
                    raise RuntimeError("Failed to process {}: {}".format(node, e))
        
        bar.set_description("Saving result of R in {}".format(fileName))
        name = "R_{}".format(fieldName)
        resultR = pd.DataFrame({"index": supplyNodes, name: list(R.values())}).set_index("index")
        resultR["fid"] = resultR.index + 1
        self.updateData(file, resultR, name)
        bar.update(3)

        bar.set_description("Saving result of A in {}".format(fileName))
        name = "A_{}".format(fieldName)
        resultA = pd.DataFrame({"index": list(A.keys()), name.format(fieldName): list(A.values())}).set_index("index")
        resultA["fid"] = resultA.index + 1
        self.updateData(file, resultA, name)
        bar.update(3)

        bar.close()
        
        return
    
if __name__ == "__main__":
    # M2SFCA().getGraph(r"test\\CHN.gpkg", "afterFlooding")
    M2SFCA().calOneLayer(r"test\\CHN.gpkg", 1000, "Gaussian", maxThreads=os.cpu_count()) # type: ignore
    M2SFCA().calOneLayer(r"test\\CHN.gpkg", 1000, "Gaussian", "afterFlooding", maxThreads=os.cpu_count()) # type: ignore