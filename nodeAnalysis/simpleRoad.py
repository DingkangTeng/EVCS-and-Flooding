import os, sys, psutil, gc
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
from shapely.geometry import LineString, Point

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

        return None
    
    @staticmethod
    def getOneCountryFromFile(
        filePath: str,
        country: str,
        savePath: str,
        customFilter: list | None = None,
        singleThread: bool = True
    ) -> None | list:
        
        if singleThread is None:
            print("Processing country: {}".format(country))

        # gdfOrigional = gpd.read_file(filePath, layer="lines", engine="pyogrio", use_arrow=True)
        gdfOrigional = gpd.read_file("_GISAnalysis\\TestData\\test.gdb", layer="rus", engine="pyogrio", use_arrow=True)
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

        if gdf.crs is not None:
            espg = gdf.crs.to_epsg()
        else:
            espg = 4326
        
        gdf = gdf[100].copy()
        
        metadata = {
            "created_date": utils.ts(),
            "created_with": f"osm.pbf from geofabrik.de",
            "crs": "epsg:{}".format(espg),
        }
        G = nx.MultiDiGraph(**metadata)
        for idx, row in gdf.iterrows():
            line: LineString = row["geometry"]
            coords = list(line.coords)
            edgeAttrs = row.drop("geometry").to_dict()
            G.add_node(coords[0], x=coords[0][0], y=coords[0][1])
            for i in range(1, len(coords)):
                u = coords[i-1]
                v = coords[i]
                G.add_node(v, x=coords[i][0], y=coords[i][1])
                G.add_edge(u, v, **edgeAttrs)
                if not row["oneway"]:
                    G.add_edge(v, u, **edgeAttrs)

        if str(espg) != "4326":
            G_proj = ox.project_graph(G, to_latlong=True)
        else:
            G_proj = G
        #Merge juncted intersections
        G2 = ox.consolidate_intersections(G_proj, rebuild_graph=True, tolerance=0.0001, dead_ends=True)
        G2 = nx.MultiDiGraph(G2, network_type="drive")

        ox.save_graph_geopackage(
            G2,
            filepath=os.path.join(savePath, "{}.gpkg".format(country)),
            directed=True,
            encoding="utf-8"
        )

        return None
    
if __name__ == "__main__":
    customFilter = " \
        [\"highway\"~\"^motorway$|^trunk$|^primary$|^secondary$|^tertiary$|^motorway_link$| \
        ^trunk_link$|^primary_link$|^secondary_link$|^tertiary_link$\"] \
    "
    # getSimpleRoad().getAllCountriesNetworksGraph("C:\\0_PolyU\\roadsGraph", customFilter=customFilter, multiThread=1) # type: ignore
    # getSimpleRoad().getOneCountry("FRANCE", "C:\\0_PolyU\\roadsGraph", customFilter=customFilter)
    customFilter = [
        "^motorway", "trunk", "primary", "secondary", "tertiary", "motorway_link",
        "trunk_link", "primary_link", "secondary_link", "tertiary_link"
    ]
    getSimpleRoad().getOneCountryFromFile("C:\\russia-latest.osm.pbf", "RUS2", "test", customFilter=customFilter)

    # Un-download:
    # 分别读取有边界点不重合的问题
    # {'CANADA',
    #  'FRANCE',
    #  'CHINA',
    #  'NORWAY',
    #  'RUSSIAN FEDERATION',
    #  'UNITED STATES OF AMERICA'}

    # Results have problem with road lenght, run calculateRoadLength after