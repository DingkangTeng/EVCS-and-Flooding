import os, sys, psutil, gc, copy
import osmnx as ox
import networkx as nx
import pandas as pd
import geopandas as gpd
import numpy as np
from iso3166 import countries_by_name
from iso3166 import countries as COUNTRIES
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from osmnx import utils
from shapely.geometry import MultiLineString, LineString, Point, MultiPoint
from shapely.strtree import STRtree
from shapely.ops import split

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, mkdir

class getSimpleRoad:
    __slots__ = ["subThreadSize", "__maxThread"]

    def __init__(self, subThreadSize: int = 1024) -> None:
        """
        Initialize the getSimpleRoad class
        """
        CPUCount = os.cpu_count()
        if CPUCount is None:
            CPUCount = 1
        memorySize = psutil.virtual_memory().available / (1024 ** 2) # One thread needs 1GB in default
        self.__maxThread = min(int(memorySize // subThreadSize), int(CPUCount ** 0.5))
        print(
            "Default multi-thread number based on the remain memeory size {}GB: {}".format(
                memorySize // 1024,
                self.__maxThread
                )
        )

        return

    def getAllCountriesNetworksGraph(
        self,
        savePath: str,
        countries: set[str] | list[str] = set(),
        customFilter: str | None = None,
        multiThread: int = 0
    ) -> None:
        """
        Get all countries networks graph and save to geopackage

        Parameters:
        savePath: The path to save the geopackage files.
        countries: A set or list of country names to process. If empty, all countries will be processed.
        customFilter: A custom filter for the OSMnx graph query. Default is None.
        multiThread: The number of threads to use for processing. Default is `0`, which means using the maximum threads based on memory size.

        Return:
        No return.
        
        This function will create a directory at `savePath` if it does not exist, and save each country's network graph as a geopackage file \
            with ISO-3166 alapha 3 code.
        If a country's geopackage file already exists in the `savePath`, it will be skipped.
        If an error occurs while processing a country, it will be logged in `exceptionCountry.csv` in the `savePath`.
        The `exceptionCountry.csv` will contain the country name, the number of exceptions, and the exception messages.
        """
        if multiThread == 0:
            multiThread = self.__maxThread

        if countries == set():
            allCountries = set(countries_by_name.keys()) # All upper
            # Exclude the countries that are already exist in the save path
            existingFiles = readFiles(savePath).specificFile(suffix=["gpkg"])
            existingCountries = set([COUNTRIES.get(f.split(".")[0]).name.upper() for f in existingFiles])
            allCountries = allCountries - existingCountries
        else:
            allCountries = set(countries)
        mkdir(savePath)  # Create the save path if not exists

        # Progress bar
        bar = tqdm(total=len(allCountries) * 2, desc="Processing countries", unit="country")

        futures = []
        futuresToCountry = {} # Store futures to country mapping for debugging
        exceptionList = []

        with ProcessPoolExecutor(max_workers=multiThread) as excutor:
            for country in allCountries:
                future = excutor.submit(
                    self.getOneCountry,
                    country,
                    savePath=savePath,
                    customFilter=customFilter,
                    singleThread = False
                )
                futures.append(future)
                futuresToCountry[future] = country  # Map future to country name
            for future in as_completed(futures):
                country = futuresToCountry[future]
                try:
                    result = future.result()
                except Exception as e:
                    tqdm.write("Error in get country {}: {}".format(country, e))
                else:
                    bar.update(1)
                    bar.set_description("{} finished".format(country))
                    if result is not None:
                        exceptionList.append(result)

        pd.DataFrame(
            exceptionList,
            columns=["Country", "Exception Times", "Exception Messages"],
        ).to_csv(
            os.path.join(savePath, "exceptionCountry.csv"),
            index=False,
            encoding="utf-8"
        )

        return
    
    @staticmethod
    def getOneCountry(
        country: str,
        savePath: str,
        truncateByEdge: bool = False,
        customFilter: str | None = None,
        singleThread: bool = True
    ) -> None | list:
        
        if singleThread:
            print("Processing country: {}".format(country))

        iso3 = countries_by_name[country].alpha3 if country in countries_by_name else country
        
        exceptionTimes = 0
        exceptionCountry = ["", 0, []]
        while True:
            if country in ["KIRIBATI", "ANTARCTICA", "NEW CALEDONIA", "PITCAIRN", "UNITED STATES MINOR OUTLYING ISLANDS"]:
                exceptionCountry = [country, 1, ["Queried too large area with sea or no data."]]
                return exceptionCountry
            elif country in [
                "HOLY SEE", "PITCAIRN", "COCOS (KEELING) ISLANDS", "SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS",
                "HEARD ISLAND AND MCDONALD ISLANDS", "TOKELAU"
            ]: # No data country
                return
            try:
                G = ox.graph_from_place(
                    country,
                    network_type="drive",
                    retain_all=True,
                    truncate_by_edge = truncateByEdge,
                    custom_filter=customFilter
                )
            except Exception as e:
                    if exceptionTimes == 0:
                        exceptionCountry[0] = country
                        exceptionCountry[1] = 1
                        exceptionCountry[2].append(str(e))
                        country = country.split(",")[0]  # Handle cases like "United States, California"
                        exceptionTimes += 1
                    elif exceptionTimes == 1:
                        exceptionCountry[1] += 1
                        exceptionCountry[2].append(str(e))
                        if singleThread:
                            print(e)
                        else:
                            tqdm.write("Error in geo-encoding after split country name to {}: {}".format(country, e))
                        
                        return exceptionCountry
            else:
                break

        G_proj = ox.project_graph(G, to_latlong=True)
        #Merge juncted intersections
        G2 = ox.consolidate_intersections(G_proj, rebuild_graph=True, tolerance=0.0001, dead_ends=True)
        G2 = nx.MultiDiGraph(G2, network_type="drive")

        ox.save_graph_geopackage(
            G2,
            filepath=os.path.join(savePath, "{}.gpkg".format(iso3)),
            directed=True,
            encoding="utf-8"
        )

        return
    
    @staticmethod
    def getOneCountryFromFile(
        filePath: str,
        country: str,
        savePath: str,
        customFilter: list | None = None
    ) -> None | list:
        
        def processLine(line: LineString, attributes: dict, nodes: dict, nodeIDs: list[int]) ->list[tuple[int, int, dict]]:
            segments = []
            coords = list(line.coords)
            attributes["geometry"] = line

            for coordxy in [coords[0], coords[-1]]:
                coord = Point(coordxy[0], coordxy[1])
                if coord not in nodes:
                    nodes[coord] = nodeIDs[0]
                    nodeIDs[0] += 1
            u = nodes[Point(coords[0])]
            v = nodes[Point(coords[-1])]
            segments.append((u, v, attributes))
            if not attributes["oneway"]:
                verseAttr = copy.deepcopy(attributes)
                verseAttr["geometry"] = LineString(coords[::-1]) # Reverse line
                segments.append((v, u, verseAttr))

            return segments
        
        tqdm.write("Processing country: {} \nExtracting data from file...".format(country))

        gdfOrigional = gpd.read_file(filePath, layer="lines", engine="pyogrio", use_arrow=True)
        # gdfOrigional = gpd.read_file("_GISAnalysis\\TestData\\test.gdb", layer="rus", use_arrow=True, rows=1000)
        if customFilter is not None:
            gdf = gdfOrigional[gdfOrigional["highway"].isin(customFilter)].copy()
            del gdfOrigional
            gc.collect()
        else:
            gdf = gdfOrigional
        gdf.drop(columns=["waterway", "aerialway", "barrier", "man_made", "railway", "z_order"], inplace=True)
        gdf["oneway"] = gdf["other_tags"].str.extract(r"\"oneway\"=>\"([^\"]*)\"")
        gdf["oneway"] = np.where(gdf["oneway"] == "yes", True, False)
        gdf["lanes"] = gdf["other_tags"].str.extract(r"\"lanes\"=>\"([^\"]*)\"")

        bar = tqdm(total=gdf.shape[0] + 410, desc=country)
        if gdf.crs is not None:
            espg = gdf.crs.to_epsg()
        else:
            espg = 4326
        
        metadata = {
            "created_date": utils.ts(),
            "created_with": f"osm.pbf from geofabrik.de",
            "crs": "epsg:{}".format(espg),
        }

        bar.set_description("Building origional Graph for {}".format(country))
        nodes: dict[Point, int] = {}
        edges = []
        nodeIDs: list[int] = [0]

        for idx, row in gdf.iterrows():
            lines = row["geometry"]
            attrs = row.drop("geometry").to_dict()
            if isinstance(lines, MultiLineString):
                for line in lines.geoms:
                    edges.extend(processLine(line, attrs, nodes, nodeIDs))
            else:
                edges.extend(processLine(lines, attrs, nodes, nodeIDs))
            bar.update(1)

        # Check middle point and split edges
        bar.set_description("Check the middle point")
        points = list(nodes.keys())
        tree = STRtree(points)

        newEdges = []
        for u, v, attr in edges:
            linestring: LineString = attr["geometry"]
            # Using STRtree find nearby points
            candidates = tree.query(linestring.buffer(1e-8))
            candidates = [points[i] for i in candidates]
            midNodes = []
            for pt in candidates:
                nodeId = nodes[pt]
                if nodeId in (u, v):
                    continue
                if linestring.distance(pt) < 1e-8:
                    proj = linestring.project(pt) # The distance of the curve from the starting point of linestring to the projection point
                    if 0 < proj < linestring.length:
                        midNodes.append((proj, nodeId, pt))
            if midNodes != []:
                midNodes.sort()
                cutPoints = MultiPoint([node[2] for node in midNodes])
                nodeIds = [u] + [node[1] for node in midNodes] + [v]
                segments = split(linestring, cutPoints).geoms
                for i in range(len(segments)):
                    newAttr = copy.deepcopy(attr)
                    newAttr["geometry"] = segments[i]
                    newEdges.append((nodeIds[i], nodeIds[i+1], newAttr))
            else:
                newEdges.append((u, v, attr))
        del edges
        gc.collect()
        bar.update(100)

        bar.set_description("Building graph...")
        G = nx.MultiDiGraph(**metadata)
        for coord, node_id in nodes.items():
            G.add_node(node_id, x=coord.x, y=coord.y)
        for u, v, attrs in newEdges:
            G.add_edge(u, v, **attrs)
        bar.update(100)
        del nodes, newEdges, nodeIDs
        gc.collect()

        if str(espg) != "4326":
            bar.set_description("Projecting {}".format(country))
            GProj = ox.project_graph(G, to_latlong=True)
            bar.update(10)
        else:
            GProj = G
            bar.update(10)

        #Merge juncted intersections
        bar.set_description("Merging juncted intersections of {}".format(country))
        G2 = ox.consolidate_intersections(GProj, rebuild_graph=True, tolerance=0.0001, dead_ends=True)
        G2 = nx.MultiDiGraph(G2, network_type="drive")
        bar.update(100)

        bar.set_description("Saving result of {}".format(country))
        ox.save_graph_geopackage(
            G2,
            filepath=os.path.join(savePath, "{}.gpkg".format(country)),
            directed=True,
            encoding="utf-8"
        )
        bar.update(100)

        bar.close()

        return
    
if __name__ == "__main__":
    customFilter = " \
        [\"highway\"~\"^motorway$|^trunk$|^primary$|^secondary$|^tertiary$|^motorway_link$| \
        ^trunk_link$|^primary_link$|^secondary_link$|^tertiary_link$\"] \
    "
    # getSimpleRoad().getAllCountriesNetworksGraph("C:\\0_PolyU\\roadsGraph", customFilter=customFilter, multiThread=1) # type: ignore
    # getSimpleRoad().getOneCountry("Honolulu", "C:\\0_PolyU\\roadsGraph", customFilter=customFilter)
    customFilter = [
        "motorway", "trunk", "primary", "secondary", "tertiary", "motorway_link",
        "trunk_link", "primary_link", "secondary_link", "tertiary_link"
    ]
    getSimpleRoad().getOneCountryFromFile("C:\\0_PolyU\\russia-latest.osm.pbf", "RUS2", "C:\\0_PolyU\\roadsGraph", customFilter=customFilter)

    # Un-download:
    # 分别读取有边界点不重合的问题
    # {'CANADA',d
    #  'FRANCE',d
    #  'CHINA',d
    #  'NORWAY',d
    #  'UNITED STATES OF AMERICA'}

    # Results have problem with road lenght, run calculateRoadLength after