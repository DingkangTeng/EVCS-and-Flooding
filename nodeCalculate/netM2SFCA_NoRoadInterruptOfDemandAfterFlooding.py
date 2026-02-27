import sys, sqlite3, os, gc
import networkx as nx
import geopandas as gpd
import pandas as pd
import numpy as np
from osmnx import convert
from tqdm import tqdm

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
    "length", "geometry", "affected"
]
SUPPLY_COLS = ["EVCSNum", "EVCSNum_After"]

def decayFunc(distance, d0, func: str) -> float:
    """Gaussian decay function"""
    if func == "Gaussian":
        return (np.exp(-0.5 * (distance / d0) ** 2)  - np.exp(-0.5)) / (1 - np.exp(-0.5))
    else:
        raise RuntimeError(" \
            Unexceptional decay function. Available function: \n \
            Gaussian decay function; \
        ")

class M2SFCA:
    __slots__ = ["file", "demandAttrs", "SupplyNodesIndex", "DemandNodesIndex", "G", "GReversed", "quit", "afterG"]

    def __init__(self, file: str, demandCol: list[str] = NODES_ATTR_POP + NODES_ATTR_POI, after: bool = False) -> None:
        tqdm.write("Initialize graph {}...".format(f"{file} after flooding" if after else file))
        self.file = file
        self.demandAttrs = demandCol
        self.quit = False
        
        # Stop calculating when do not have evcs data
        nodesColumns = gpd.read_file(file, layer="nodes", encoding="utf-8", rows=0).columns
        edgesColumns = gpd.read_file(file, layer="edges", encoding="utf-8", rows=0).columns
        if "EVCSNum" not in nodesColumns or "EVCSNum_After" not in nodesColumns or "affected" not in edgesColumns:
            self.quit = True
            return
        
        nodes = gpd.read_file(file, layer="nodes", encoding="utf-8").set_index("osmid")
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
        
        self.SupplyNodesIndex = nodes[self.getBool(nodes, SUPPLY_COLS)].index.to_numpy()
        self.DemandNodesIndex = nodes[self.getBool(nodes, demand)].index.to_numpy()
        self.G = convert.graph_from_gdfs(nodes, edges)
        if after:
            edges = edges[edges["affected"] == 0]
            self.GReversed = convert.graph_from_gdfs(nodes, edges).reverse()
        else:
            self.GReversed = self.G.reverse()
        self.afterG = after

        del nodes, edges
        gc.collect()
        tqdm.write("Initialize graph {} finished.".format(file))

        return
    
    @staticmethod
    def getBool(df: pd.DataFrame, colList: list[str]) -> np.ndarray:
        condition = np.zeros(df.shape[0], dtype=bool)
        for col in colList:
            colCondition = df[col].notna().to_numpy() & (df[col] != 0).to_numpy()
            condition = condition | colCondition

        return condition
    
    @staticmethod
    def updateData(path: str, df: pd.DataFrame, fieldNames: list[str]) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        fields = ((field, "Real", None, False) for field in fieldNames)
        cursor.addFields("nodes", *fields)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON nodes (fid)")

        # Reset update column to NULL
        setNull = ", ".join([f"{field} = NULL" for field in fieldNames])
        cursor.execute(f"UPDATE nodes SET {setNull}")
        
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
    def __demandDijkstra(G: nx.MultiDiGraph, node: int, d0: float, decay: str, demandAttrs: list[str]) -> np.ndarray:
        result = np.zeros([len(demandAttrs), len(SUPPLY_COLS)],dtype=np.float64)
        # Calculate the accessable demand points from supply node within distance d0
        try:
            paths = nx.single_source_dijkstra_path_length(G, node, cutoff=d0, weight="length")
        # No access path
        except nx.NetworkXNoPath:
            paths = {node: 0}
        
        for k, supplyCol in enumerate(SUPPLY_COLS):
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
        
        return result
    
    @staticmethod
    def __supplyDijKstra(
        GReversed: nx.MultiDiGraph,
        node: int,
        d0: float, decay: str,
        supplyNodes: set[int],
        R: dict[int, np.ndarray],
        demandAttrs: list[str]
    ) -> np.ndarray | None:
        demandValues = []
        for demandAttr in demandAttrs:
            demandValue = GReversed.nodes[node].get(demandAttr, 0)
            demandValues.append(demandValue)
        if sum(demandValues) == 0: return None

        result = np.zeros([len(demandAttrs), 2],dtype=np.float64)

        # Calculate the accessable supply points from demand node within distance d0
        try:
            reversePaths = nx.single_source_dijkstra_path_length(GReversed, node, cutoff=d0, weight="length")
        # No access path
        except nx.NetworkXNoPath:
            reversePaths = {node: 0}

        for i, distance in reversePaths.items():
            weightVal = decayFunc(distance, d0, decay)
            if i in supplyNodes:
                data = R.get(i, None) # Only consider supply nodes
            else: continue
        
            for j, demandValue in enumerate(demandValues):
                if demandValue == 0: continue
                for k in range(2):
                    result[j][k] += data[j][k] * weightVal * weightVal if isinstance(data, np.ndarray) else 0
            
        return result.flatten()
    
    def calOneLayer(self, d0: float, decayFunc: str) -> bool:
        """
        filter:
        afterFlooding: calculates all population after flooding
        ...
        """
        if self.quit: return False

        fileName = os.path.basename(self.file)
        # Save cols name
        cols = [x.split('_')[-1] for x in self.demandAttrs]
        if self.afterG:
            cols = [item for pair in [(f"{x}_AfterG", f"{x}_AfterG_After") for x in cols] for item in pair]
        else:
            cols = [item for pair in [(x, f"{x}_After") for x in cols] for item in pair]
        ## x: no flooding; x_After: only EVCS are affeceted; x_AfterG: only roads are affected; x_AfterG_After: both EVCS and roads are affected

        bar = tqdm(
            total=len(self.DemandNodesIndex) + len(self.SupplyNodesIndex) + 1,
            desc="Calcunating Demand in {}".format(fileName)
        )
        
        # Supply ratios
        R = {} # node idx: [supply before, supply after]
        for node in self.SupplyNodesIndex:
            R[node] = self.__demandDijkstra(self.G, node, d0, decayFunc, self.demandAttrs)
            bar.update(1)

        supplyNodes = set(self.SupplyNodesIndex)

        # Accessibility
        bar.set_description("Calcunating Supply in {}".format(fileName))
        A = {}
        for node in self.DemandNodesIndex:
            result = self.__supplyDijKstra(self.GReversed, node, d0, decayFunc, supplyNodes, R, self.demandAttrs)
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
    
def calAllLayer(path: str, d0: int, decayFunc: str, col: list[str] = NODES_ATTR_POP + NODES_ATTR_POI) -> None:
    log = loadJsonRecord(os.path.join(path, "log.json"), "M2SFCA_{}".format(d0), [])
    processed = log.get()
    gpkgs = readFiles(path).specificFile(["gpkg"])
    gpkgs.sort()
    
    n =len(gpkgs)
    for i, gpkg in enumerate(gpkgs):
        if gpkg in processed: continue
        tqdm.write("Processing {}({}/{})".format(gpkg, i + 1, n))
        
        result = False
        for after in [True, False]:
            result = M2SFCA(os.path.join(path,gpkg), col, after=after).calOneLayer(d0, decayFunc)
        
        log.append(gpkg) if result else None
        log.save()

    return
    

if __name__ == "__main__":
    # a = M2SFCA(r"C:\0_PolyU\roadsGraph_BeijinInner\CHN.gpkg")
    # a.calOneLayer(1000, "Gaussian", "population_All") # type: ignore
    # a.calOneLayer(1000, "Gaussian", "population_All", after=True, maxThreads=os.cpu_count()) # type: ignore
    D0 = 3000
    calAllLayer(r"C:\\0_PolyU\\roadsGraph", D0, "Gaussian")

    from analysis import mergeData, calUpperLevel
    mergeData(
        "C:\\0_PolyU\\roadsGraph", (r"_GISAnalysis\\Dissertation.gdb", "GAUL_2024_L2"), ("iso3_code", "disp_en"), D0
    ).mergeAll("C:\\0_PolyU\\test")

    ANALY_RESULT = r"C:\0_PolyU\test\3km"
    MERGE_RESULT = r"C:\0_PolyU\test\merge_3km.parquet"
    calUpperLevel(MERGE_RESULT, ANALY_RESULT, 10, "city").agg("city", 16)