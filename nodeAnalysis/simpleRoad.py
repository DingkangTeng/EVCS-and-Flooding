import os, sys, psutil, threading
import osmnx as ox
import networkx as nx
import pandas as pd
from iso3166 import countries_by_name
from iso3166 import countries as COUNTRIES
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

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
            existingFiles = readFiles(savePath).specifcFile(suffix=["gpkg"])
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
        exceptionLock = threading.Lock()
        excutor = ThreadPoolExecutor(max_workers=multiThread)

        for PN in allCountries:
        #     future = excutor.submit(
        #         self.getOneCountry,
        #         PN,
        #         savePath=savePath,
        #         customFilter=customFilter,
        #         multiThread=(bar, exceptionList, exceptionLock)
        #     )
        #     futures.append(future)
        #     futuresToCountry[future] = PN  # Map future to country name
        # for future in as_completed(futures):
        #     try:
        #         future.result()
        #     except Exception as e:
        #         tqdm.write("Error in get country {}: {}".format(futuresToCountry[future], e))

            self.getOneCountry(
                PN,
                savePath=savePath,
                customFilter=customFilter,
                multiThread=(bar, exceptionList, exceptionLock)
            )
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
        PN: str,
        savePath: str,
        customFilter: str | None = None,
        multiThread: tuple[tqdm, list[list], threading.Lock] | None = None,
        # bar, exceptionList, exceptionLock
    ) -> None | list:
        
        if multiThread is None:
            print("Processing country: {}".format(PN))
        else:
            multiThread[0].set_description("Processing country: {}".format(PN))
            multiThread[0].update(1)
        iso3 = countries_by_name[PN].alpha3 if PN in countries_by_name else PN
        
        exceptionTimes = 0
        exceptionCountry = ["", 0, []]
        while True:
            if PN == "KIRIBATI":
                exceptionCountry = ["KIRIBATI", 1, ["KIRIBATI queried too large area with sea."]]
                return exceptionCountry
            try:
                G = ox.graph_from_place(
                    PN,
                    network_type="drive",
                    retain_all=True,
                    custom_filter=customFilter
                )
            except Exception as e:
                    if exceptionTimes == 0:
                        exceptionCountry[0] = PN
                        exceptionCountry[1] = 1
                        exceptionCountry[2].append(str(e))
                        PN = PN.split(",")[0]  # Handle cases like "United States, California"
                        exceptionTimes += 1
                    elif exceptionTimes == 1:
                        exceptionCountry[1] += 1
                        exceptionCountry[2].append(str(e))
                        if multiThread is None:
                            print(e)
                        else:
                            with multiThread[2]:
                                multiThread[1].append(exceptionCountry)
                            tqdm.write("Error in geo-encoding after split country name to {}: {}".format(PN, e))
                            multiThread[0].update(1)
                        
                        return exceptionCountry
            else:
                break

        G_proj = ox.project_graph(G)
        #Merge juncted intersections
        G2 = ox.consolidate_intersections(G_proj, rebuild_graph=True, tolerance=10, dead_ends=True)
        G2 = nx.MultiDiGraph(G2, network_type="drive")

        ox.save_graph_geopackage(
            G2,
            filepath=os.path.join(savePath, "{}.gpkg".format(iso3)),
            directed=True,
            encoding="utf-8"
        )

        if multiThread is not None:
            multiThread[0].update(1)

        return None
    
if __name__ == "__main__":
    customFilter = " \
        [\"highway\"~\"^motorway$|^trunk$|^primary$|^secondary$|^tertiary$|^motorway_link$| \
        ^trunk_link$|^primary_link$|^secondary_link$|^tertiary_link$\"] \
    "
    getSimpleRoad().getAllCountriesNetworksGraph("C:\\0_Data\\globalRoad", customFilter=customFilter, multiThread=1) # type: ignore

    # Following steps are not implemented yet
    # Nodes -> Tissen polygons -> connect nodes with EVCS and population