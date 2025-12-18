import sys, os
import geopandas as gpd
import numpy as np
from tqdm import tqdm

sys.path.append(".") # Set path to the roots

from _function.readFiles import loadJsonRecord

class mergeData:
    __slots__ = ["roadsPath", "countries", "boundary"]

    def __init__(self, roadsPath: str, boundaryPath: str | tuple[str, str], boundaryCols: tuple[str, str]) -> None:
        self.roadsPath = roadsPath
        self.countries = loadJsonRecord(
            os.path.join(roadsPath, "log.json"),
            "M2SFCA"
        ).get()

        if isinstance(boundaryPath, str):
            self.boundary = gpd.read_file(boundaryPath, encoding="utf-8", usecols=list(boundaryCols) + ["geometry"])
        else:
            self.boundary = gpd.read_file(boundaryPath[0], layer=boundaryPath[1], encoding="utf-8")
            self.boundary = self.boundary[list(boundaryCols) + ["geometry"]]

    def mergeAll(self, savePath: str) -> None:
        n = len(self.countries)

        dfs = np.empty([n], dtype=gpd.GeoDataFrame)
        country: str
        skip = 0
        bar = tqdm(total=n, desc="Merging data", unit="country")
        for i, country in enumerate(self.countries):
            bar.set_description("Merging {}".format(country))
            path = os.path.join(self.roadsPath, country)
            edges = gpd.read_file(path, layer="edges", encoding="utf-8").set_index('u')
            # Sikp country with no flooding record
            if "affected" not in edges.columns:
                skip += 1
                bar.update()
                continue

            nodes = gpd.read_file(path, layer="nodes", encoding="utf-8").set_index("osmid")
            A = []
            P = []
            for col in nodes.columns:
                A.append(col) if col[0:2] == "A_" \
                    else P.append(col) if col[0:10] == "population" or (col[0:3] == "POI" and col[-4:] != "Fids") or col[0:11] == "otherRaster" \
                    else None
            # Sikp country have no accessibility
            if nodes[A].sum().sum() == 0:
                skip += 1
                bar.update()
                continue

            nodes = nodes[["osmid_original", "EVCSNum", "EVCSNum_After", "geometry"] + A + P] # do not need R
            
            edges = edges[["affected", "affectedIncident", "city"]]
            edges = edges[~edges.index.duplicated(keep="first")]
            nodes = nodes.join(edges)
            countryName = country.split('.')[0]

            # Find isolated nodes' city
            isolate = nodes.loc[nodes["city"].isna()]
            if not isolate.shape[0] == 0:
                # Change edges to city dict for affected incident
                edges = edges.drop_duplicates(subset="city").set_index("city")

                # Spatial join with cities polygon
                isolate = gpd.sjoin(
                    isolate, 
                    self.boundary[self.boundary["iso3_code"] == countryName], 
                    how="left",
                    predicate="within"
                )

                nodes.loc[isolate.index, "city"] = isolate["disp_en"]
                del isolate
                nodes.dropna(subset="city", inplace=True) # Drop isolated nodes which out of boundary

                # Fill affected and affectedIncident
                for col in ["affected", "affectedIncident"]:
                    nodes[col] = nodes[col].fillna(
                        nodes["city"].map(edges[col].to_dict())
                    )

            # Fill country code
            nodes["iso3"] = countryName

            dfs[i-skip] = nodes
            bar.update()
        
        dfs = dfs[0: -skip]
        df = gpd.pd.concat(dfs)

        # Delete city having no flooding affected record
        df["EVCSChange"] = df["EVCSNum_After"] / df["EVCSNum"] * 100 - 100
        cityStats = df.groupby("city").agg({
            "affected": "sum",
            "EVCSChange": "sum",
        }).reset_index()
        cityStats = cityStats[
            (cityStats["affected"] == 0) & 
            (cityStats["EVCSChange"] == 0)
        ]["city"].tolist()
        df = df[~df["city"].isin(cityStats)]

        gpd.GeoDataFrame(df, crs=dfs[0].crs).to_file(
            os.path.join(savePath, "merge.gpkg"),
            layer="nodes",
            encoding="utf-8"
        )
        # Save data individually for quickly access 
        df.drop(columns=["geometry"]).to_parquet(os.path.join(savePath, "merge.parquet"), compression="gzip")

        return

# Debug
if __name__ == "__main__":
    a = mergeData("C:\\0_PolyU\\roadsGraph", (r"_GISAnalysis\\Dissertation.gdb", "GAUL_2024_L2"), ("iso3_code", "disp_en"))
    a.mergeAll("C:\\0_PolyU\\test")
    