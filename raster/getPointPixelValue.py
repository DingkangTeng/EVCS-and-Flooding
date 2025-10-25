import sys, os, sqlite3
import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio as rio
from tqdm import tqdm

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, loadJsonRecord
from function.sqlite import spatialiteConnection, modifyTable, FID_INDEX

class getPointPixelsValue:
    __slots__ = ["roadRoot", "gpkgs", "evcs", "rasterRoot", "evcsPath"]

    def __init__(self, roadNetPath: str, evcsPath: str | tuple[str, str], rasterRoot: str) -> None:
        self.roadRoot = roadNetPath
        self.rasterRoot = rasterRoot
        self.gpkgs = readFiles(roadNetPath).specificFile(["gpkg"])
        self.gpkgs.sort()

        if isinstance(evcsPath, str):
            self.evcs = gpd.read_file(evcsPath, encoding="utf-8", usecoles=["level1", "geometry"])
            self.evcsPath = evcsPath
        else:
            self.evcs = gpd.read_file(evcsPath[0], layer=evcsPath[1], encoding="utf-8")
            self.evcs = self.evcs[["geometry"]]
            self.evcsPath = evcsPath[0]
        self.evcs["fid"] = self.evcs.index + 1
        
        return
    
    @staticmethod
    def updateData(path: str, df: pd.DataFrame) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # Add field
        cursor.addFields(
            "evcs",
            ("floodingValue", "Integer", 0, False),
            ("incident", "Text", None, False)
        )
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON evcs (fid)")
        conn.commit()
        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False, method="multi", chunksize=10922) #32766//3
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        conn.commit()
        cursor.execute(
            f"""
            UPDATE evcs
            SET floodingValue = tempTable.value, incident = tempTable.incident
                FROM tempTable 
                WHERE tempTable.fid = evcs.fid
            """
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

        return
    
    def calAllCountry(self):
        log = loadJsonRecord(os.path.join(os.path.dirname(self.evcsPath), "log.json"), "Flooding_Influence", [])
        processed = log.get()
        n = len(self.gpkgs)
        for i, gpkg in enumerate(self.gpkgs):
            # Skip processed
            if gpkg in processed: continue

            path = os.path.join(self.roadRoot, gpkg)
            edges = gpd.read_file(path, layer="edges", encoding="utf-8")
            # Sikp not affected country
            if "affected" not in edges.columns:
                log.append(gpkg)
                log.save()
                continue
            
            cities = edges["city"].unique()
            nodes = gpd.read_file(path, layer="nodes", encoding="utf-8")
            # Skip country which do not have evcs:
            if "EVCSFids" not in nodes.columns:
                log.append(gpkg)
                log.save()
                continue
            
            results = []
            bar = tqdm(total=len(cities) + 20, desc="Processing {}({}/{})".format(gpkg, i, n), unit="city")
            for city in cities:
                # Get nodes which are affected by flooding
                subEdges: pd.DataFrame = edges[edges["city"] == city]
                incident: str = subEdges["affectedIncident"].iloc[0]
                subnodes = nodes[nodes["osmid"].isin(np.union1d(subEdges['u'], subEdges['v']))].dropna(subset="EVCSFids")
                # Skip cities with no evcs
                if len(subnodes) == 0:
                    bar.update()
                    continue
                # Get evcs id list from the affected nodes
                evcsFid = np.unique(np.concatenate(subnodes["EVCSFids"].str.split(',').values).astype(int))
                result = self.getOneIncident(evcsFid, incident)
                results.append(result)
                bar.update()

            if len(results) == 0:
                bar.update(10)
            else:
                results =np.vstack(results)
                results = pd.DataFrame(results, columns=["fid", "value", "incident"]).sort_values(by="value").drop_duplicates(subset="fid")
                mask = results["value"].isna()
                results.loc[mask] = results[mask].fillna(0)
                bar.update(10)
                # Update Data
                self.updateData(self.evcsPath, results)

            log.append(gpkg)
            log.save()
            bar.update(10)
            bar.close()

        return
    
    def getOneIncident(self, evcsFid: np.ndarray, incident: str) -> np.ndarray:
        # Convert file name like "DFO_2947_From_20060803_to_200610110000000000-0000014848.tif.tif"
        split = incident.split('_')
        if len(split) != 6:
            # The col of CHN have suffix with "_3" lke "DFO_2947_From_20060803_to_200610110000000000_3"
            # which needs to be cleaned
            incident = '-'.join(incident.rsplit('_', 1)) if len(split[-1]) != 1 else incident[:-2]
        rasterPath = os.path.join(self.rasterRoot, incident + ".tif.tif")

        # Get raster data withing the layer extent
        with rio.open(rasterPath) as src:
            evcs = self.evcs[self.evcs["fid"].isin(evcsFid)]
            evcs = gpd.GeoDataFrame(evcs["fid"], geometry=evcs["geometry"], crs=self.evcs.crs)
            # Check reference system
            if evcs.crs != src.crs:
                evcs.to_crs(src.crs, inplace=True)
            XCoords = evcs.geometry.x.to_numpy().astype(np.float32)
            YCoords = evcs.geometry.y.to_numpy().astype(np.float32)

            coords = np.array(list(zip(XCoords, YCoords)))
            values = np.array([val[0] for val in src.sample(coords)])
            
            return np.column_stack((evcs[["fid"]], values, [incident] * evcs.shape[0]))
            # [[fid, days/times， incident name]]

# Debug
if __name__ == "__main__":
    # a = getPointPixelsValue(
    #     "C:\\0_PolyU\\test",
    #     ("C:\\0_PolyU\\test\\merge.gpkg", "evcs"),
    #     "D:\\floodingAll_Days"
    # ).calAllCountry()
    getPointPixelsValue(
        "C:\\0_PolyU\\roadsGraph",
        ("C:\\0_PolyU\\global_EVCS.gpkg", "evcs"),
        "D:\\floodingAll_Days"
    ).calAllCountry()