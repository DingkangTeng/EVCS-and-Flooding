import os, sys, warnings, gc
import geopandas as gpd
import numpy as np
import rasterio as rio
from osmnx import convert
from networkx import density
from rasterio.mask import mask
from sklearn.neighbors import BallTree
from shapely.geometry.base import BaseGeometry
from shapely.geometry import Polygon, box
from pyproj import CRS
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from threading import Lock

sys.path.append(".") # Set path to the roots
warnings.filterwarnings(
    "ignore",
    message="Discarding the `gdf_nodes` 'geometry' column"
)

from _plot import plt, BAR_COLORS
from analysis.__readNode import readNode

warnings.filterwarnings(
    "ignore", 
    category=UserWarning,
    message="Geometry is in a geographic CRS.*"
)

R = 6371.0  # earth radius km

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
    
    def EVCS(self, evcs: str | tuple[str, str] | gpd.GeoDataFrame, maxThread:int = 1) -> None:
        # Add density
        self.df["EVCSDensity"] = self.df["EVCSNum"] / self.df["area"] * 1000000 # counts/km2

        # Add coverage
        evcs = self.__readFile(evcs, [])
        self.df["EVCScoverage"] = np.nan
        self.df["EVCSAggregation"] = np.nan # Nearest Neighbor Index, NNI, from 0 to 1, 0 means aggregate and 1 means saperate
        bar = tqdm(total=self.df.shape[0], desc="Calculating EVCS coverage", unit="city")

        assert self.df.crs is not None
        if evcs.crs != self.df.crs: evcs.to_crs(self.df.crs, inplace=True) # type: ignore

        intersections = np.empty([self.df.shape[0]], dtype=gpd.GeoDataFrame)
        n = 0
        futures = []
        idxDict = {}
        
        with ProcessPoolExecutor(max_workers=maxThread) as executor:
            for row in self.df.itertuples():
                idx = getattr(row, "Index")
                boundary: BaseGeometry = getattr(row, "geometry")
                area = getattr(row, "area")

                # Get intersection
                possibleMatchesIdx = list(evcs.sindex.intersection(boundary.bounds))
                if possibleMatchesIdx:
                    possibleMatches = evcs.iloc[np.array(possibleMatchesIdx)]
                    evcsWithin = possibleMatches[possibleMatches.within(boundary)]
                else:
                    evcsWithin = gpd.GeoDataFrame(geometry=[], crs=evcs.crs)
                
                future = executor.submit(self._getEVCSIntersection, evcsWithin, boundary, area, self.df.crs)
                futures.append(future)
                idxDict[future] = idx

            for future in as_completed(futures):
                idx = idxDict[future]
                try:
                    coverage, intersection, aggregation = future.result()
                except Exception as e:
                    raise RuntimeError(f"{idx}: {e}")
                else:
                    self.df.at[idx, "EVCScoverage"] = coverage
                    self.df.at[idx, "EVCSAggregation"] = aggregation
                    # Save coverage shape
                    if intersection is not None:
                        intersections[n] = intersection
                        n += 1
                    bar.update()
        
        bar.close()

        # Save EVCS coverage geometry
        interDf = gpd.pd.concat(intersections[:n], ignore_index=True)
        interDf = gpd.GeoDataFrame(interDf, crs=self.df.crs).drop_duplicates("geometry").reset_index()
        bar = tqdm(total=interDf.shape[0], desc="Saving EVCS coverage geometry", unit="city")

        # Delete overlap gird
        sidx = interDf.sindex
        keep = np.empty([interDf.shape[0]], dtype=np.uint32)
        n = 0

        for i, geom in enumerate(interDf.geometry):
            if geom is None or geom.is_empty: continue

            # Find intersections with spatial index
            possibleIdx = set(sidx.intersection(geom.bounds))
            
            ## Only check with previously kept geometries
            kept = True
            for j in keep[0:n]:
                if j in possibleIdx:
                    if geom.intersects(interDf.geometry.iloc[j]):
                        kept = False
                        break
            
            # If no intersection with previously kept geometries, keep it
            if kept:
                keep[n] = i
                n += 1

            bar.update()
            
        interDf.loc[keep[0:n]].reset_index(drop=True).to_file(
            self.savePath[0], layer="EVCSCoverage", encoding="utf-8", layer_options={'OVERWRITE': 'YES'}
        )

        bar.close()

        return
    
    @staticmethod
    def _getEVCSIntersection(
        evcsWithin: gpd.GeoDataFrame, boundary: BaseGeometry, boundaryArea: float,
        crs: CRS, WIDTH = 0.01 # Around 1km in WGS84
    ) -> tuple[float, None | gpd.GeoSeries, float]:
        if evcsWithin.empty: return 0, None, np.nan
        else:
            minx, miny, maxx, maxy = boundary.bounds
            
            xCells = int(np.ceil((maxx - minx) / WIDTH))
            yCells = int(np.ceil((maxy - miny) / WIDTH))

            grid = np.empty([xCells * yCells], dtype=Polygon)
            i = 0
            for x0 in np.arange(minx, maxx, WIDTH):
                for y0 in np.arange(miny, maxy, WIDTH):
                    x1 = x0 + WIDTH
                    y1 = y0 + WIDTH
                    cell = box(float(x0), float(y0), float(x1), float(y1))
                    grid[i] = cell
                    i += 1

            gridGdf = gpd.GeoDataFrame(geometry=grid[:i], crs=crs)
            gridGdf = gridGdf[gridGdf.intersects(boundary)]

            # Calculate dissloved buffer
            gridGdf = gridGdf.sjoin(evcsWithin, how="left")
            intersection = gridGdf[gridGdf["index_right"].notna()]

        # NNI
        n = evcsWithin.shape[0]
        if n < 2: aggregation = np.nan
        else:
            coords = np.array([(p.y, p.x) for p in evcsWithin.geometry]) # type: ignore
            coordsRad = np.radians(coords)

            # Calculate the nearest distance for each point
            tree = BallTree(coordsRad, metric="haversine")
            distances, _ = tree.query(coordsRad, k=2) # k=2 menas the nearest two points（the fist is itself）
            nearest_dists = distances[:, 1] * R  # The second is the nearest

            obs_mean = np.mean(nearest_dists)          # Real average nearest distance (km)
            density = n / boundaryArea * 1000000       # density (count/km2)
            exp_mean = 0.5 / np.sqrt(density)          # Except nearest distance
            aggregation = obs_mean / exp_mean

        if intersection.empty: return 0, None, aggregation
        else: return intersection.shape[0] / gridGdf.shape[0], intersection.geometry, aggregation
    
    def road(self, roadRoot: str, evcs: str | tuple[str, str] | gpd.GeoDataFrame, maxThread:int = 1) -> None:
        self.df["roadLength"] = np.nan
        self.df["roadDensity"] = np.nan
        self.df["roadCoverage"] = np.nan
        self.df["roadsLengthChange"] = np.nan
        self.df["roadConnectivity"] = np.nan # Graph Density

        evcs = self.__readFile(evcs, ["level1"])
        iso3 = self.df["iso3"].unique().tolist()
        bar = tqdm(total=self.df.shape[0] * 2, desc="Calculating road related indicator", unit="city")

        futures = []
        futuresDict = {}
        executor = ProcessPoolExecutor(max_workers=maxThread)
        for country in iso3:
            bar.set_description(f"Reading file for {country}")
            road = os.path.join(roadRoot, "{}.gpkg".format(country))
            roadDf = self.__readFile((road, "edges"), usecoles=["length", "affected", 'u', 'v', "key"], index=["city"])
            roadNode = self.__readFile((road, "nodes"), usecoles=['x', 'y'], index=["osmid"])
            if roadDf.crs != self.df.crs: roadDf.to_crs(self.df.crs, inplace=True) # type: ignore

            subDf: gpd.GeoDataFrame = self.df.loc[self.df["iso3"] == country]
            subEVCS: gpd.GeoDataFrame = evcs.loc[evcs["level1"] == country]

            for row in subDf.itertuples():
                idx = getattr(row, "Index")
                area = getattr(row, "area")
                boundary: BaseGeometry = getattr(row, "geometry")
                roads: gpd.GeoDataFrame = roadDf.loc[idx]
                roadsNodes = roadNode[roadNode.index.isin(gpd.pd.unique(roads[["u", "v"]].values.ravel('K')))]

                future = executor.submit(
                    self._singleRoad, roads, area, boundary, roadsNodes, subEVCS
                )
                futures.append(future)
                futuresDict[future] = idx
                bar.update()

        for future in as_completed(futures):
            bar.set_description(f"Calculating indexs")
            idx = futuresDict[future]
            try:
                resutls = future.result()
            except Exception as e:
                raise RuntimeError(e)
            else:
                self.df.at[idx, "roadDensity"] = resutls[0]
                self.df.at[idx, "roadCoverage"] = resutls[1]
                self.df.at[idx, "roadLength"] = resutls[2]
                self.df.at[idx, "roadsLengthChange"] = resutls[3]
                self.df.at[idx, "roadConnectivity"] = resutls[4]
                bar.update()
        
        bar.close()
        executor.shutdown()

        return
    
    @staticmethod
    def _singleRoad(
        roads: gpd.GeoDataFrame,
        area: np.floating, boundary: BaseGeometry,
        roadsNodes: gpd.GeoDataFrame,
        subEVCS: gpd.GeoDataFrame
    ) -> tuple:
        if roads.shape[0] == 0:
            return 0, 0, 0, 0, 0, 0
        else:
            length = roads["length"].sum() / 1000

            # Roads density
            roadDensity = length / area * 1000000 # km/km2

            # Coverage
            roadsBuffer = roads.geometry.buffer(0.01).union_all()
            intersection = roadsBuffer.intersection(boundary)
            roadCoverage = intersection.area / boundary.area
            del roadsBuffer, intersection
            gc.collect()

            # Roads length change
            if subEVCS.shape[0] == 0:
                roadsLengthChange = np.nan
            else:
                buffer = subEVCS.geometry.buffer(0.01).union_all()
                roadsAroundEVCS = roads.iloc[roads.sindex.query(buffer, predicate="intersects")]
                lengthAfter = roadsAroundEVCS.loc[roadsAroundEVCS["affected"] == 0, "length"].sum()
                lengthBefor = roadsAroundEVCS["length"].sum()
                roadsLengthChange = (lengthAfter - lengthBefor) / lengthBefor * 100 if lengthBefor > 0 else 0
                del buffer, roadsAroundEVCS

            # roadConnectivity, by graph density (edge count / [node count * (node count - 1)])
            roadConnectivity = density(
                convert.graph_from_gdfs(
                    roadsNodes,
                    roads.set_index(["u", "v", "key"])
                )
            )

        del roads
        gc.collect()

        return roadDensity, roadCoverage, length, roadsLengthChange, roadConnectivity
    
    def population(self, rasterRoot: str, maxThread: int = 1) -> None:
        # Density
        self.df["populationDensity"] = self.df["population_All"] / self.df["area"]

        # Coverage
        self.df["populationCV"] = np.nan
        iso3 = self.df["iso3"].unique().tolist()
        bar = tqdm(total=self.df.shape[0], desc="Calculating population coverage", unit="city")

        futures = []
        lock = Lock()
        with ThreadPoolExecutor(max_workers=maxThread) as executor:
            for country in iso3:
                subDf: gpd.GeoDataFrame = self.df.loc[self.df["iso3"] == country]
                raster = os.path.join(rasterRoot, "{}_allGender_allAge_merge.tif".format(country))
                futures.append(
                    executor.submit(self.__popCoverage, subDf, raster, lock, bar)
                )

        return
    
    def __popCoverage(self, subDf: gpd.GeoDataFrame, rasterRoot: str, lock: Lock, bar: tqdm) -> None:
        with rio.open(rasterRoot, options=["NUM_THREADS=ALL_CPUS"]) as pop:
            for row in subDf.itertuples():
                idx = getattr(row, "Index")
                geom = getattr(row, "geometry")

                raster, _ = mask(pop, [geom], crop=True, nodata=pop.nodata, all_touched=True)
                values: np.ndarray = raster[0].flatten()
                values = values[values != pop.nodata]
                values = values[~np.isnan(values)]

                # CV
                with lock:
                    if len(values) > 0:
                        self.df.loc[idx, "populationCV"] = np.std(values) / np.mean(values)
                    else:
                        self.df.loc[idx, "populationCV"] = 0
                    
                    bar.update()

        return
    
    def flooding(self, floodingBinary: str) -> None:
        self.df["folldingCoverage"] = np.nan
        bar = tqdm(total=self.df.shape[0], desc="Reading flooding area", unit="city")

        flooding = gpd.read_file(floodingBinary, encoding="utf-8")
        if flooding.crs != self.df.crs: flooding.to_crs(self.df.crs, inplace=True) # type: ignore

        bar.set_description("Calculating flooding coverage")

        for row in self.df.itertuples():
            idx = getattr(row, "Index")
            boundary: BaseGeometry = getattr(row, "geometry")

            # Coverage
            possibleMatchesIdx = list(flooding.sindex.intersection(boundary.bounds))
            if possibleMatchesIdx:
                possibleMatches = flooding.iloc[np.array(possibleMatchesIdx)]
                intersection = possibleMatches.intersection(boundary)
                self.df.at[idx, "folldingCoverage"] = intersection.union_all().area / boundary.area
            else:
                self.df.at[idx, "folldingCoverage"] = 0
        
            bar.update()

        return
    
# Debug
if __name__ == "__main__":
    root = r"C:\\0_PolyU\\test\\1km"
    CITY_RESULT = os.path.join(root, "city.csv")
    COUNTRY_RESULT = os.path.join(root, "city.csv")
    GEO_DB = r"_GISAnalysis\Dissertation.gdb"
    EVCS = (r"C:\0_PolyU\global_EVCS.gpkg", "evcs")
    DOWN_ROAD = os.path.join(r"C:\0_PolyU", "roadsGraph")
    SAVE_PATH = r"E:\Population_Related"
    
    a = EVCSIndicator(CITY_RESULT, (GEO_DB, "GAUL_2024_L2"), (r"C:\\0_PolyU\\test\\indicator.gpkg", "city"))
    a.EVCS(EVCS, 32)
    # a.road(DOWN_ROAD, EVCS, 8)
    # a.population(os.path.join(SAVE_PATH, "population_All"), 16)
    # a.flooding(r"E:\Flooding_Related\flooding\floodingArea.shp")
    a.save()
    # print(a.df.columns)