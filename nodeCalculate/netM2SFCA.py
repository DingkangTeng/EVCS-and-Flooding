import sys, sqlite3, os, gc
import networkx as nx
import geopandas as gpd
import pandas as pd
import numpy as np
from osmnx import convert
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.append(".") # Set path to the roots

from _function.readFiles import readFiles, loadJsonRecord
from _function.sqlite import spatialiteConnection, modifyTable, FID_INDEX

NODES_ATTR = [
        "x", "y", "geometry", "EVCSNum", "EVCSNum_After"
    ]
NODES_ATTR_POP = [
    "population_All_children", "population_All_young", "population_All_middle", "population_All_elderly",
    "population_Male", "population_Female",
    "population_All", "otherRaster_landscan_global_2024"
]
NODES_ATTR_POI = [
    "POI_1Num", "POI_2Num", "POI_3Num", "POI_POIAll"
]
EDGES_ATTR = [
    "length", "geometry"
]

def decayFunc(distance, d0, func: str) -> float:
    """Gaussian decay function"""
    if func == "Gaussian":
        return np.exp(-0.5 * (distance / d0) ** 2)
    else:
        raise RuntimeError(" \
            Unexceptional decay function. Available function: \n \
            Gaussian decay function; \
        ")

class M2SFCA:
    __slots__ = ["file", "demandAttrs", "nodesIndex", "G", "GReversed", "quit"]

    def __init__(self, file: str, demandCol: list[str] = NODES_ATTR_POP + NODES_ATTR_POI) -> None:
        tqdm.write("Initialize graph {}...".format(file))
        self.file = file
        self.demandAttrs = demandCol
        self.quit = False
        nodes = gpd.read_file(file, layer="nodes", encoding="utf-8").set_index("osmid")
        # Stop calculating when do not have evcs data
        if "EVCSNum" not in nodes.columns or "EVCSNum_After" not in nodes.columns:
            self.quit = True
            return
        # Calculate POI_All
        if "POI_POIAll" not in nodes.columns:
            nodes["POI_POIAll"] = nodes[["POI_1Num", "POI_2Num", "POI_3Num"]].sum(axis=1)

        # Stop calculating when do not have demand data
        demand = []
        for i in demandCol:
            if i in nodes.columns: demand.append(i)
        if len(demand) == 0:
            self.quit = True
            return
        nodes = nodes[NODES_ATTR + demand]

        edges = gpd.read_file(file, layer="edges", encoding="utf-8").set_index(['u', 'v', "key"])[EDGES_ATTR]
        
        self.nodesIndex = nodes.index.to_numpy()
        self.G = convert.graph_from_gdfs(nodes, edges)
        self.GReversed = self.G.reverse()

        del nodes, edges
        gc.collect()
        tqdm.write("Initialize graph {} finished.".format(file))

        return
    
    @staticmethod
    def updateData(path: str, df: pd.DataFrame, fieldNames: list[str]) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        fields = ((field, "Real", None, False) for field in fieldNames)
        cursor.addFields("nodes", *fields)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON nodes (fid)")
        # Add data
        df.to_sql(
            "tempTable",
            conn, if_exists="replace",
            index=False,
            method="multi", chunksize=32766//((len(fieldNames)+1)) #32766//2 (n columns + 1 fid)
        )
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        conn.commit()
        setClause = ", ".join([f"{field} = tempTable.{field}" for field in fieldNames])
        cursor.execute(
            f"""
            UPDATE nodes
            SET {setClause}
                FROM tempTable
                WHERE tempTable.fid = nodes.fid
            """
        )
        cursor.execute(f"DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

        return
    
    @staticmethod
    def __demandDijkstra(G: nx.MultiDiGraph, node: int, d0: float, decay: str, demandAttrs: list[str], supplyCols: list[str]) -> np.ndarray:
        result = np.zeros([len(demandAttrs), len(supplyCols)],dtype=np.float64)
        # Calculate the accessable demand points from supply node within distance d0
        try:
            paths = nx.single_source_dijkstra_path_length(G, node, cutoff=d0, weight="length")
        # No access path
        except nx.NetworkXNoPath:
            paths = {node: 0}
        
        for k, supplyCol in enumerate(supplyCols):
            EVCSnum = G.nodes[node].get(supplyCol, None)
            if EVCSnum is None: continue
            
            for j, demandAttr in enumerate(demandAttrs):
                totalWeightedDemand = 0
                for i, distance in paths.items():
                    demandValue = G.nodes[i].get(demandAttr, 0)
                    weightVal = decayFunc(distance, d0, decay)
                    totalWeightedDemand += demandValue * weightVal

                if totalWeightedDemand > 0:
                    result[j][k] = EVCSnum / totalWeightedDemand
                else:
                    result[j][k] = 0
        
        return result.flatten()
    
    @staticmethod
    def __supplyDijKstra(
        GReversed: nx.MultiDiGraph,
        node: int,
        d0: float, decay: str,
        supplyNodes: set[int],
        R: dict[int, np.ndarray],
        demandAttrs: list[str],
        supplyCols: list[str]
    ) -> np.ndarray | None:
        demandValues = []
        for demandAttr in demandAttrs:
            demandValue = GReversed.nodes[node].get(demandAttr, 0)
            demandValues.append(demandValue)
        if sum(demandValues) == 0: return None

        supplyN = len(supplyCols)
        result = np.zeros([len(demandAttrs), supplyN],dtype=np.float64)

        # Calculate the accessable supply points from demand node within distance d0
        try:
            reversePaths = nx.single_source_dijkstra_path_length(GReversed, node, cutoff=d0, weight="length")
        # No access path
        except nx.NetworkXNoPath:
            reversePaths = {node: 0}

        for i, distance in reversePaths.items():
            weightVal = decayFunc(distance, d0, decay)
            data = R.get(i, None) if i in supplyNodes else None # Only consider supply nodes
            if data is None: continue
        
            for j, demandValue in enumerate(demandValues):
                if demandValue == 0: continue
                for k in range(supplyN):
                    result[j][k] += data[2 * j + k] * weightVal * weightVal if isinstance(data, np.ndarray) else 0
            
        return result.flatten()
    
    def calOneLayer(self, d0: float, decayFunc: str, after: bool = False) -> bool:
        """
        filter:
        afterFlooding: calculates all population after flooding
        ...
        """
        if self.quit: return False
        supplyCols = ["EVCSNum", "EVCSNum_After"] if after else ["EVCSNum"]

        fileName = os.path.basename(self.file)
        cols = [x.split('_')[-1] for x in self.demandAttrs]
        if after:
            cols = [item for pair in [(x, f"{x}_After") for x in cols] for item in pair]

        bar = tqdm(
            total=len(self.nodesIndex) * 2 + 3,
            desc="Calcunating Demand in {}".format(fileName)
        )
        
        # Supply ratios
        R = {}
        for node in self.nodesIndex:
            R[node] = self.__demandDijkstra(self.G, node, d0, decayFunc, self.demandAttrs, supplyCols)
            bar.update(1)

        supplyNodes = set(R.keys())

        # Accessibility
        bar.set_description("Calcunating Supply in {}".format(fileName))
        A = {}
        for node in self.nodesIndex:
            result = self.__supplyDijKstra(self.GReversed, node, d0, decayFunc, supplyNodes, R, self.demandAttrs, supplyCols)
            if result is not None: A[node] = result
            bar.update(1)

        # Save A
        bar.set_description("Saving result of A in {}".format(fileName))
        aCols = ["A_{}".format(x) for x in cols]
        resultA = pd.DataFrame.from_dict(A, orient="index", columns=aCols)
        resultA["fid"] = resultA.index + 1
        self.updateData(self.file, resultA, aCols)
        bar.update(1)

        bar.close()
        
        return True
    
def calAllLayer(path: str, d0: int, decayFunc: str, col: list[str] = NODES_ATTR_POP + NODES_ATTR_POI, maxThread: int = 1) -> None:
    log = loadJsonRecord(os.path.join(path, "log.json"), "M2SFCA", [])
    processed = log.get()
    gpkgs = readFiles(path).specificFile(["gpkg"])
    gpkgs.sort()
    
    n =len(gpkgs)
    # futures = []
    # debugDict = {}

    # executor = ProcessPoolExecutor(max_workers=maxThread)
    # for i, gpkg in enumerate(gpkgs):
    #     if gpkg in processed: continue
    #     tqdm.write("Processing {}({}/{})".format(gpkg, i + 1, n))
        
    #     intit = M2SFCA(os.path.join(path, gpkg), col)
    #     future = executor.submit(intit.calOneLayer, d0, decayFunc, after=True)
    #     futures.append(future)
    #     debugDict[future] = gpkg

    # for future in as_completed(futures):
    #     gpkg = debugDict[future]
    #     try:
    #         result = future.result()
    #     except Exception as e:
    #         raise RuntimeError(f"Failed process {gpkg}: {e}")
    #     else:
    #         log.append(gpkg) if result else None
    #         log.save()
    for i, gpkg in enumerate(gpkgs):
        if gpkg in processed: continue
        tqdm.write("Processing {}({}/{})".format(gpkg, i + 1, n))
        
        result = M2SFCA(os.path.join(path,gpkg), col).calOneLayer(d0, decayFunc, after=True)
        
        log.append(gpkg) if result else None
        log.save()

    return
    
if __name__ == "__main__":
    # a = M2SFCA(r"C:\0_PolyU\roadsGraph_BeijinInner\CHN.gpkg")
    # a.calOneLayer(1000, "Gaussian", "population_All") # type: ignore
    # a.calOneLayer(1000, "Gaussian", "population_All", after=True, maxThreads=os.cpu_count()) # type: ignore
    calAllLayer(r"C:\\0_PolyU\\roadsGraph", 5000, "Gaussian", maxThread=8)