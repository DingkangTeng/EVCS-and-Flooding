import sys, os, sqlite3
import geopandas as gpd
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

sys.path.append(".") # Set path to the roots

from _function.sqlite import spatialiteConnection, modifyTable, FID_INDEX
from vectorCalculate.__calculationFun import *

class addCityProperty:
    __slots__ = [
        "boundary", "maxThread",
        "addFields", "countries"
    ]

    def __init__(self, boundaryPath: str | tuple[str, str], boundaryCols: list[str], maxThread: int = 1) -> None:
        if isinstance(boundaryPath, str):
            self.boundary = gpd.read_file(boundaryPath, encoding="utf-8", usecols=boundaryCols + ["geometry"])
        else:
            self.boundary = gpd.read_file(boundaryPath[0], layer=boundaryPath[1], encoding="utf-8")
            self.boundary = self.boundary[boundaryCols + ["geometry"]]
        
        self.boundary.set_index(boundaryCols[0]) # Set the fist col as index

        self.addFields = []
        self.countries = self.boundary["iso3_code"].unique().tolist()
        self.maxThread = maxThread
        
        return
    
    @staticmethod
    def updateData(path: str, df: pd.DataFrame, fiedlNames: list[str]) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # Add field
        fields = ((field, "REAL", 0, False) for field in fiedlNames)
        cursor.addFields("boundary", *fields)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON boundary (fid)")
        conn.commit()

        # Add data
        df[fiedlNames].to_sql(
            "tempTable", conn,
            if_exists="replace", index=False,
            method="multi", chunksize=int(327664//(len(fiedlNames))+1) # 32766//columns
        )
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        conn.commit()
        conn.execute("BEGIN TRANSACTION;")
        setStr = ",".join([f"{fieldName} = tempTable.{fieldName}" for fieldName in fiedlNames]) # {fieldName} = tempTable.{fieldName} like
        cursor.execute(
            f"""
            UPDATE boundary
            SET {setStr}
                FROM tempTable 
                WHERE tempTable.fid = boundary.fid
            """
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

        return
    
    def saveResults(self, savePath: str) -> None:
        savePath = os.path.join(savePath, "boundary.gpkg")
        if os.path.exists(savePath):
            self.updateData(savePath, self.boundary, self.addFields)
        else:
            self.boundary.to_file(savePath, layer="boundary", encoding="utf-8")

    def addRoadDens(self, roadsRoot: str) -> None:
        self.boundary["roadDens"] = np.nan
        bar = tqdm(total=len(self.countries), desc="Calculating road density", unit="country")

        futures = []
        futureDict = {}
        with ProcessPoolExecutor(max_workers=self.maxThread) as executor:
            for country in self.countries:
                road = os.path.join(roadsRoot, "{}.gpkg".format(country))
                if not os.path.exists(road):
                    bar.update()
                    continue

                subdf = self.boundary.loc[self.boundary["iso3_code"] == country, ["geometry", "roadDens", "area"]]
                
                future = executor.submit(roadDens, road, subdf)
                futures.append(future)
                futureDict[future] = country
            
            for future in as_completed(futures):
                country = futureDict[future]
                try:
                    result: pd.Series = future.result()
                except Exception as e:
                    raise RuntimeError("{}: {}".format(country, e))
                else:
                    self.boundary.loc[result.index, "roadDens"] = result
                    bar.update()

        # If all success
        bar.close()
        self.addFields.append("roadDens")

        return

#Debug
if __name__ == "__main__":
    a = addCityProperty((r"_GISAnalysis\Dissertation.gdb", "GAUL_2024_L2"), ["disp_en", "iso3_code", "area"], 8)

    # a.addRoadDens(r"C:\0_PolyU\roadsGraph") # finished

    a.saveResults(r"C:\0_PolyU")