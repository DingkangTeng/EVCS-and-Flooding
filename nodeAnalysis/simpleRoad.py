import os, sys, psutil, threading
import osmnx as ox
import networkx as nx
import pandas as pd
from iso3166 import countries_by_name
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, mkdir

class getSimpleRoad:
    __slots__ = ["subThreadSize", "__maxThread"]

    def __init__(self, subThreadSize: int = 1024):
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

    def getAllCountriesNetworksGraph(self, savePath: str, customFilter: str | None = None, multiThread: int = 0) -> None:
        """
        Get all countries networks graph and save to geopackage
        :param savePath: The path to save the geopackage
        """
        if multiThread == 0:
            multiThread = self.__maxThread

        allCountries = set(countries_by_name.keys())
        mkdir(savePath)  # Create the save path if not exists
        # Exclude the countries that are already exist in the save path
        existingFiles = readFiles(savePath).specifcFile(suffix=["gpkg"])
        existingCountries = set([f.split(".")[0] for f in existingFiles])
        allCountries = allCountries - existingCountries

        # Progress bar
        bar = tqdm(total=len(allCountries) * 2, desc="Processing countries", unit="country")

        futures = []
        futuresToCountry = {} # Store futures to country mapping for debugging
        exceptionList = []
        exceptionLock = threading.Lock()
        excutor = ThreadPoolExecutor(max_workers=multiThread)

        for PN in allCountries:
            future = excutor.submit(
                self.getOneCountry,
                PN,
                savePath=savePath,
                customFilter=customFilter,
                multiThread=(bar, exceptionList, exceptionLock)
            )
            futures.append(future)
            futuresToCountry[future] = PN  # Map future to country name
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                tqdm.write("Error in get country {}: {}".format(futuresToCountry[future], e))

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
        iso3 = countries_by_name[PN].alpha3
        
        exceptionTimes = 0
        exceptionCountry = ["", 0, []]
        while True:
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
    getSimpleRoad().getAllCountriesNetworksGraph("test", customFilter, multiThread=os.cpu_count()) # type: ignore

    # Following steps are not implemented yet
    # Nodes -> Tissen polygons -> connect nodes with EVCS and population