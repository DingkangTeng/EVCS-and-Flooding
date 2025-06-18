import sys, os, zipfile, threading, psutil, gc, shutil
import rasterio as rio
import pandas as pd
import numpy as np
# import warp as wp
from rasterio.io import MemoryFile
from rasterio.merge import merge
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import override

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, mkdir
from raster.floodingMerge import floodingMerge

class populationMerge(floodingMerge):

    def mergeAll( # type: ignore
        self,
        savePath: str,
        mainAge: list[int] = [x for x in range(0, 81, 5)] + [1],
        gender: list[str] = ['m','f'],
        multiThread: int = 0
    ) -> None:
        
        countries = readFiles(self.path).allFolder()
        saved = [] # Save path for all countries
        n = len(countries)
        bar = tqdm(total=n+10, desc="Starting")
        i = 1
        if multiThread == 0:
            multiThread = self.__maxThread

        # A thread-safe way to append results
        savedLock = threading.Lock()
        def subThread(country: str, i: int, n: int, saved: list[str]) -> None:
            tqdm.write("Processing {} ({}/{})".format(country, i, n))
            result = self.mergeByAge(
                country,
                savePath,
                mainAge=mainAge,
                gender=gender,
                bar=bar
            )
            if result is not None:
                with savedLock:
                    saved.append(result)

            return

        futures = []
        futuresToCountry = {} # Store futures to country mapping for debugging
        excutor = ThreadPoolExecutor(max_workers=multiThread)
        for country in countries:
            future = excutor.submit(subThread, country, i, n, saved)
            futures.append(future)
            futuresToCountry[future] = country
            i += 1
        for future in as_completed(futures):
            country = futuresToCountry[future]
            try:
                future.result()
            except Exception as e:
                tqdm.write("Error in merge country {}: {}".format(country, e))

        # Different countries may have overlap，so use max (union)
        bar.set_description("Merging all countries...")
        datasets = [rio.open(fp) for fp in saved]
        bar.update(1)
        mosaic, out_transform = merge(datasets, method="max", nodata=0)
        bar.update(6)
        outMeta = datasets[0].meta.copy()
        outMeta["height"] = mosaic.shape[1]
        outMeta["width"] = mosaic.shape[2]
        outMeta["transform"] = out_transform
        outMeta["nodata"] = 0
        # Compress
        outMeta["compress"] = "DEFLATE"
        outMeta["zlevel"] = 9
        outMeta["predictor"] = 2 # Flot using 3
        outMeta["num_threads"] = "all_cpus"
        with rio.open(os.path.join(savePath, "0_All_merge.tif"), 'w', **outMeta) as dst:
            dst.write(mosaic)
        for ds in datasets:
            ds.close()
        bar.update(3)
        
        return
    
    @override
    def mergeOneCountry(self, country: str, savePath: str, mainBand: int, multiThread: int = 0) -> None:
        """
        This function is not implemented in populationMerge, use mergeByAge instead.
        """
        print("This function is not implemented in populationMerge, use mergeByAge instead.")

        return

    def mergeByAge(
            self,
            country: str,
            savePath: str,
            mainAge: list[int] = [x for x in range(0, 81, 5)] + [1],
            gender: list[str] = ['m','f'],
            bar: None | tqdm = None
        ) -> (str | None):

        mainAge.sort()
        path = os.path.join(self.path, country)
        mkdir(savePath)

        files = readFiles(path).specifcFile(suffix=["tif"])
        datas = []
        for file in files:
            attr = file.split("_")
            if attr[1] in gender and int(attr[2]) in mainAge:
                datas.append(file)
        if len(datas) == 0:
            tqdm.write("No tif files found in {}".format(country))
            return

        datasets = [rio.open(os.path.join(path, fp)) for fp in datas]
        if bar is None:
            tqdm.write("Mosacing {}".format(country))
        else:
            bar.set_description("Mosacing {}".format(country))
        # Estimates of total number of people per grid square broken down by gender and age groupings, therefor use sum to merge
        mosaic, out_transform = merge(datasets, method="sum", nodata=np.nan) # Simiart to Mosaic To New Raster (Data Management) in ArcGIS
        
        outMeta = datasets[0].meta.copy()
        outMeta["height"] = mosaic.shape[1]
        outMeta["width"] = mosaic.shape[2]
        outMeta["transform"] = out_transform
        outMeta["nodata"] = 0
        # Compress
        outMeta["compress"] = "DEFLATE"
        outMeta["zlevel"] = 9
        outMeta["predictor"] = 3 # Int using 2, Flot using 3
        outMeta["num_threads"] = "all_cpus"
        result = os.path.join(
            savePath, "{}_{}_{}_merge.tif".format(
                country,
                gender if len(gender) == 1 else "allGender",
                mainAge if len(mainAge) != 18 else "allAge"
            )
        )
        with rio.open(result, 'w', **outMeta) as dst:
            dst.write(mosaic)
        for ds in datasets:
            ds.close()

        # Save metadata
        metadata = pd.DataFrame({"File Names": datas})
        metadata.to_csv(os.path.join(savePath, "{}_metadata.csv".format(country)), encoding="utf-8")

        if bar is not None:
            bar.update(1)

        return result

if __name__ == "__main__":
    # Adjust the multi-thread number based on your computer, too much threads will cause memory overflow
    # populationMerge(r"C:\\0_PolyU\\population\\").mergeByAge("AGO", "test")
    populationMerge(r"C:\\0_PolyU\\population\\").mergeAll("test", multiThread=2)