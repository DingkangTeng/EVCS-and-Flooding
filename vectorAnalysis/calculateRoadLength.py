import sys, sqlite3
import geopandas as gpd
from pyproj import Geod

sys.path.append(".") # Set path to the roots

from function.sqlite import spatialiteConnection, FID_INDEX, modifyTable

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
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

        return
    
    @staticmethod
    def calculateAll(path: str) -> None:
        # ...
        return

# Debug
if __name__ == "__main__":
    calculateRoadLength.calculateOneGpkg(r"test\\CHN.gpkg")