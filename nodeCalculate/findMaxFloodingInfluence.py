import sys, os, gc, sqlite3
import geopandas as gpd
import pandas as pd
import networkx as nx
from osmnx import convert
from shapely.geometry.base import BaseGeometry
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from threading import Lock

sys.path.append(".") # Set path to the roots

from _function.readFiles import readFiles, loadJsonRecord
from _function.sqlite import spatialiteConnection, modifyTable, FID_INDEX

class findMaxFloodingInfluence:
    __slots__ = ["gpkgPath", "boundary", "countryCol", "descCol"]

    def __init__(self, gpkgPath: str, boundaryPath: str | tuple[str, str], boundaryCols: tuple[str, str]) -> None:
        self.gpkgPath = gpkgPath
        self.countryCol, self.descCol = boundaryCols
        if isinstance(boundaryPath, str):
            self.boundary = gpd.read_file(boundaryPath, encoding="utf-8", usecols=list(boundaryCols) + ["geometry"])
        else:
            self.boundary = gpd.read_file(boundaryPath[0], layer=boundaryPath[1], encoding="utf-8")
            self.boundary = self.boundary[list(boundaryCols) + ["geometry"]]

    @staticmethod
    def updateData(path: str, df: pd.DataFrame) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # Add field
        cursor.addFields("edges",
            ("affectedIncident", "Text", None, False),
            ("affected", "Integer", 0, False),
            ("city", "Text", None, False)
        )
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON edges (fid)")
        conn.commit()

        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False, method="multi", chunksize=8191) #32766//4 (four columns)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        conn.commit()
        conn.execute("BEGIN TRANSACTION;")
        for fieldName in ["affectedIncident", "affected", "city"]:
            cursor.execute(
                f"""
                UPDATE edges
                SET {fieldName} = tempTable.{fieldName}
                    FROM tempTable 
                    WHERE tempTable.fid = edges.fid
                """
            )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()
        del df
        gc.collect()

        return

    def processOneGpkg(self, gpkg: str) -> None:
        # Read gpkg
        gpkgPath = os.path.join(self.gpkgPath, gpkg)
        df = gpd.read_file(gpkgPath, layer="edges")
        bar = tqdm(total=df.shape[0] + 500, desc="Processing {}".format(gpkg), unit="roads")
        processedDays = df.columns[df.columns.str.contains("DFO_")].tolist()
        # Processed uninfulenced country directly
        if len(processedDays) == 0:
            return
        
        for i in processedDays:
            df[i] *= df["length"]
        sindex = df.sindex
        
        # Get boundaries
        bar.set_description("Get {} boundaries".format(gpkg))
        if self.boundary.crs != df.crs and df.crs is not None:
            self.boundary.to_crs(df.crs, inplace=True)
        boundary: gpd.GeoDataFrame = self.boundary.loc[self.boundary[self.countryCol] == gpkg.split('.')[0]]
        bar.update(100)

        elementResults: dict[int, dict] = {}
        processedNodes = set()
        # Cal the sum of incident by cities boundary and mark the max influence incident
        for i in boundary.itertuples():
            bound: BaseGeometry = getattr(i, "geometry")

            # Search by spatial index
            possibleIndex = sindex.intersection(bound.bounds)
            possibleMatches = df.iloc[possibleIndex]
            # Accuracy search
            subdf = possibleMatches[possibleMatches.intersects(bound)]

            # Find the max
            colSums = subdf[processedDays].sum()
            maxCol = colSums.idxmax()
            colValues = df.loc[subdf.index, maxCol]

            # Update result
            for idx in subdf.index:
                val = colValues[idx]
                if idx not in elementResults or val > elementResults.get(idx, {}).get("affected", -1):
                    elementResults[idx] = {
                        "affectedIncident": maxCol,
                        "city": getattr(i, self.descCol),
                        "affected": val
                    }
                    processedNodes.add(getattr(subdf.loc[idx], "u"))
                    processedNodes.add(getattr(subdf.loc[idx], "v"))
                    bar.update(1)
        
        # Check unprocessed road
        del sindex, boundary
        gc.collect()
        processedIndex = elementResults.keys()
        outbound = df.iloc[~df.index.isin(processedIndex)]
        df["index"] = df.index # add index for search
        if outbound.shape[0] != 0:
            bar.set_description("Read {} graph data".format(gpkg))
            # Build graph and get all processed node
            node = gpd.read_file(gpkgPath, layer="nodes")
            G = convert.graph_from_gdfs(
                node[["osmid" , "x", "y", "geometry"]].set_index("osmid"),
                df.set_index(["v", "u", "key"])[["length", "index", "geometry"]] # Reversing direction graph
            )
            del node
            gc.collect()
            bar.update(50)

            bar.set_description("Process {} outboundary roads".format(gpkg))
            for row in outbound.itertuples():
                v = getattr(row, 'v')
                idx = getattr(row, "Index")
                # Find one nearst processed edges
                try:
                    distances = nx.single_source_dijkstra_path_length(G, v, weight="length", cutoff=10000) # distances has been sorted
                    for x in distances:
                        breakTag = False
                        if x in processedNodes:
                            for u in G.neighbors(x):
                                nearEdgeIndex = G.edges[(x, u, 0)]["index"]
                                if nearEdgeIndex in processedIndex:
                                    processdData = elementResults[nearEdgeIndex]
                                else: continue
                                elementResults[idx] = {
                                    "affectedIncident": processdData["affectedIncident"],
                                    "city": processdData["city"],
                                    "affected": outbound.loc[idx, processdData["affectedIncident"]]
                                }
                                breakTag = True
                                break
                        if breakTag:
                            bar.update(1)
                            break
                except (nx.NetworkXNoPath, nx.NodeNotFound):
                    continue

            outbound = df.iloc[~df.index.isin(elementResults.keys())]

        # Fill iosloated data with its own max value
        if outbound.shape[0] != 0:
            bar.set_description("Process {} isolated roads".format(gpkg))
            for row in outbound.itertuples():
                idx = getattr(row, "Index")
                colValues = outbound.loc[idx, processedDays]
                maxCol = colValues.idxmax()
                elementResults[idx] = {
                    "affectedIncident": maxCol,
                    "city": "Isolated",
                    "affected": outbound.at[idx, maxCol] # type: ignore
                }
                bar.update(1)

        # Update data
        bar.set_description("Saving data for {}".format(gpkg))
        result = pd.DataFrame(elementResults.values(), index=list(elementResults.keys()))
        result["fid"] = result.index + 1
        self.updateData(gpkgPath, result)
        bar.n = bar.total
        bar.refresh()
        bar.close()

        return

    def processAll(self, threadNum: int = 1) -> None:
        log = loadJsonRecord(os.path.join(self.gpkgPath, "log.json"), "Find_Max_Influence", [])
        gpkgs = readFiles(self.gpkgPath).specificFile(["gpkg"])
        gpkgs.sort()
        
        bar = tqdm(total=len(gpkgs), desc="Processing all gpkgs", unit="gpkg")
        if threadNum > 1:
            futures = []
            debugDict = {}
            with ProcessPoolExecutor(max_workers=threadNum) as executor:
                for gpkg in gpkgs:
                    if gpkg in log: bar.update(1) # Skip the processed
                    else:
                        future = executor.submit(self.processOneGpkg, gpkg)
                        futures.append(future)
                        debugDict[future] = gpkg
                    
                for future in as_completed(futures):
                    gpkg = debugDict[future]
                    try: future.result()
                    except Exception as e:
                        raise RuntimeError("Error in processing {}: {}".format(gpkg, e))
                    else:
                        with Lock():
                            log.append(gpkg)
                            log.save()
                            bar.update(1)
        else:
            for gpkg in gpkgs:
                if gpkg in log: bar.update(1) # Skip the processed
                else:
                    self.processOneGpkg(gpkg)
                    log.append(gpkg)
                    log.save()
                    bar.update(1)

        bar.close()
        return

# Debug
if __name__ == "__main__":
    # findMaxFloodingInfluence(r"C:\\0_PolyU\\test", (r"_GISAnalysis\\Dissertation.gdb", "GAUL_2024_L2"), ("iso3_code", "disp_en")).processOneGpkg("JPN.gpkg")
    findMaxFloodingInfluence(r"C:\\0_PolyU\\roadsGraph", (r"_GISAnalysis\\Dissertation.gdb", "GAUL_2024_L2"), ("iso3_code", "disp_en")).processAll(1)