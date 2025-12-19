import os, sys, zipfile
import geopandas as gpd
import numpy as np
import rasterio as rio
from rasterio.merge import merge
from shapely.geometry.base import BaseGeometry
from tqdm import tqdm

sys.path.append(".") # Set path to the roots

from _plot import plt, BAR_COLORS
from analysis.__readNode import readNode
from _function.readFiles import readFiles

class EVCSIndicator:
    __slots__ = ["df", "savePath"]

    def __init__(
        self,
        result: str,
        boundary: str | tuple[str, str] | gpd.GeoDataFrame,
        savePath: tuple[str, str],
        minEvcsNum: int = 0
    ) -> None:
        if os.path.exists(savePath[0]): df = gpd.read_file(savePath[0], layer=savePath[1], encoding="utf-8").set_index("city")
        else:
            df, _, _ = readNode(result, minEvcsNum)
            # Add EVCS count and geometry
            df = df.set_index(
                "city"
            ).join(
                self.__readFile(boundary, ["area"], ["disp_en"])
            )[["iso3", "area", "EVCSNum", "population_All", "geometry"]]
        
        self.df = gpd.GeoDataFrame(df, crs=4326)
        self.savePath = savePath

        return
    
    def save(self) -> None:
        self.df.to_file(self.savePath[0], layer=self.savePath[1], encoding="utf-8")

        return
    
    @staticmethod
    def __readFile(path: str | tuple[str, str] | gpd.GeoDataFrame, usecoles: list[str], index: list[str] | None = None) -> gpd.GeoDataFrame:
        cols = usecoles+["geometry"]+index if index is not None else usecoles+["geometry"]
        if isinstance(path, str):
            result = gpd.read_file(path, encoding="utf-8", usecols=cols)
        elif isinstance(path, gpd.GeoDataFrame):
            result = path[cols]
        else:
            result = gpd.read_file(path[0], layer=path[1], encoding="utf-8")
            result = result[cols]

        if index is not None: result.set_index(index, inplace=True)
        
        return result
    
    def EVCS(self, evcs: str | tuple[str, str] | gpd.GeoDataFrame) -> None:
        # Add density
        self.df["EVCSDensity"] = self.df["EVCSNum"] / self.df["area"]

        # Add coverage
        evcs = self.__readFile(evcs, [])
        self.df["EVCScoverage"] = np.nan
        bar = tqdm(total=self.df.shape[0], desc="Calculating EVCS coverage", unit="city")

        assert self.df.crs is not None
        if evcs.crs != self.df.crs: evcs.to_crs(self.df.crs, inplace=True) # type: ignore
        for row in self.df.itertuples():
            idx = getattr(row, "Index")
            boundary: BaseGeometry = getattr(row, "geometry")

            # Get intersection
            possibleMatchesIdx = list(evcs.sindex.intersection(boundary.bounds))
            if possibleMatchesIdx:
                possibleMatches = evcs.iloc[np.array(possibleMatchesIdx)]
                evcs = possibleMatches[possibleMatches.within(boundary)]
            else:
                evcs = gpd.GeoDataFrame(geometry=[], crs=evcs.crs)

            if len(evcs) == 0: self.df.at[idx, "EVCScoverage"] = 0
            else:
                # Calculate dissloved buffer
                points = evcs.geometry.buffer(0.01).union_all()
                intersection = points.intersection(boundary)
                if intersection.is_empty: self.df.at[idx, "EVCScoverage"] = 0
                else: self.df.at[idx, "EVCScoverage"] = intersection.area / boundary.area

            bar.update()

        bar.close()
        return
    
    def road(self, roadRoot: str) -> None:
        self.df["roadDensity"] = np.nan
        self.df["roadCoverage"] = np.nan
        self.df["roadLength"] = np.nan
        iso3 = self.df["iso3"].unique().tolist()
        bar = tqdm(total=self.df.shape[0], desc="Calculating road density", unit="city")

        for country in iso3:
            road = os.path.join(roadRoot, "{}.gpkg".format(country))
            roadDf = self.__readFile((road, "edges"), usecoles=["length"], index=["city"])
            if roadDf.crs != self.df.crs: roadDf.to_crs(self.df.crs, inplace=True) # type: ignore
            subDf: gpd.GeoDataFrame = self.df.loc[self.df["iso3"] == country]

            for row in subDf.itertuples():
                idx = getattr(row, "Index")
                area = getattr(row, "area")
                boundary: BaseGeometry = getattr(row, "geometry")
                roads: gpd.GeoDataFrame = roadDf.loc[idx]

                if roads.shape[0] == 0:
                    self.df.at[idx, "roadDensity"] = 0
                    self.df.at[idx, "roadCoverage"] = 0
                    self.df.at[idx, "roadLength"] = 0
                else:
                    length = roads["length"].sum() / 1000
                    self.df.at[idx, "roadLength"] = length
                    self.df.at[idx, "roadDensity"] = length / area
                    # Coverage
                    roadsBuffer = roads.geometry.buffer(0.01).union_all()
                    intersection = roadsBuffer.intersection(boundary)
                    self.df.at[idx, "roadCoverage"] = intersection.area / boundary.area
                
                bar.update()
        
        bar.close()        
        return
    
    def population(self) -> None:
        # Density
        self.df["populationDensity"] = self.df["population_All"] / self.df["area"]

        # Coverage
        self.df["populationCoverage"] = np.nan
        iso3 = self.df["iso3"].unique().tolist()

        for country in iso3:
            subDf: gpd.GeoDataFrame = self.df.loc[self.df["iso3"] == country]
            ...

        return
    
    def flooding(self, floodingBinary: str) -> None:
        self.df["folldingCoverage"] = np.nan

        flooding = gpd.read_file(floodingBinary, encoding="utf-8")
        if flooding.crs != self.df.crs: flooding.to_crs(self.df.crs, inplace=True) # type: ignore

        self.df["area_a"] = self.df.area
        intersection = gpd.overlay(self.df, flooding, how="intersection", keep_geom_type=True)
        intersection["area_intersection"] = intersection.area
        
        # Calculate the intersection area
        overlap = intersection.groupby(intersection.index).agg({
            "area_intersection": "sum"
        })
        
        # 合并回原始gdf_a
        self.df = self.df.merge(overlap, left_index=True, right_index=True, how="left")
        
        # 计算重合占比
        self.df["area_intersection"].fillna(0, inplace=True)
        self.df["folldingCoverage"] = (self.df["area_intersection"] / self.df["area_a"]) * 100
        self.df.drop(columns=["area_a", "area_intersection"], inplace=True)

        return
    
    def plot(self, savePath: str = "") -> None:

        return
    
# Debug
if __name__ == "__main__":
    root = r"C:\\0_PolyU\\test\\1km"
    CITY_RESULT = os.path.join(root, "city.csv")
    COUNTRY_RESULT = os.path.join(root, "city.csv")
    GEO_DB = r"_GISAnalysis\Dissertation.gdb"
    EVCS = (r"C:\0_PolyU\global_EVCS.gpkg", "evcs")
    DOWN_ROAD = os.path.join(r"C:\0_PolyU", "roadsGraph")
    
    a = EVCSIndicator(CITY_RESULT, (GEO_DB, "GAUL_2024_L2"), (r"C:\\0_PolyU\\test\\indicator.gpkg", "city"))
    a.EVCS(EVCS)
    a.road(DOWN_ROAD)
    # a.population()
    a.flooding(r"E:\Flooding_Related\flooding\floodingArea.shp")
    a.save()
    # print(a.df.columns)