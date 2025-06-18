import sys, os, zipfile, threading, psutil, gc, shutil
import rasterio as rio
import pandas as pd
import numpy as np
# import warp as wp
from rasterio.io import MemoryFile
from rasterio.merge import merge
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, mkdir

class floodingMerge:
    __slot__ = ["path", "subThreadSize"]
    data = []
    __maxThread: int = 1

    def __init__(self, path: str, subThreadSize: int = 1024):
        """
        Initialization setting

        Parameters:
        path: The root path of flooding tif file
        subThreadSize: Arrange how much memeory will cost in each sub-thread, \
        the unit is MB and default value is 1024 MB.

        Retruns:
        None
        """
        # wp.init()
        self.path = path
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
        
    def mergeAll(self, savePath: str, mainBand: int, multiThread: int = 0) -> None:
        countries = readFiles(self.path).allFolder()
        saved = [] # Save path for all countries
        n = len(countries)
        i = 1
        if multiThread == 0:
            multiThread = self.__maxThread

        # A thread-safe way to append results
        savedLock = threading.Lock()
        def subThread(country: str, i: int, n: int, saved: list[str]) -> None:
            tqdm.write("Processing {} ({}/{})".format(country, i, n))
            result = self.mergeOneCountry(country, savePath, mainBand)
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
        tqdm.write("Merging all countries...")
        datasets = [rio.open(fp) for fp in saved]
        mosaic, out_transform = merge(datasets, method="max", nodata=0)
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
        
        return

    def mergeOneCountry(self, country: str, savePath: str, mainBand: int, multiThread: int = 0) -> str | None:
        path = os.path.join(self.path, country)
        tmpPath = os.path.join(path, "tmp")
        mkdir(savePath)
        mkdir(tmpPath)
        if multiThread == 0:
            multiThread = self.__maxThread

        files = readFiles(path).specifcFile(suffix=["zip"])
        n = len(files)
        if n == 0:
            tqdm.write("No tif files found in {}".format(country))
            return
        
        datas = []
        datasLock = threading.Lock()
        bar = tqdm(total=n+10, desc="Starting", postfix=country)

        def readTif(datas: list, path: str, file: str, mainBand: int, bar: tqdm, n: int):
            bar.set_description("Removing permanent water bodies ({} files)".format(n))
            zipPath = os.path.join(path, file)
            z = zipfile.ZipFile(zipPath, 'r')
            tifs = [x for x in z.namelist() if x.split('.')[-1] == "tif"]
            for tif in tifs:
                flooding = z.read(tif)
                dataset = MemoryFile(flooding).open() # Read tif file in memory
                meta = dataset.meta
                """
                Useful band
                Band 1: flooded
                Band 2: flood_duration
                Band 5: jrc_perm_water (1 - permanent water, 0 - non-water)
                """

                # Exclude permanent water
                ## mask = ~B5 # Change permanent water into 0, and no water into 1
                ## result = B1 * mask
                # @wp.kernel
                # def removePW(
                #     data: wp.array(dtype=int), # type: ignore
                #     premWater: wp.array(dtype=wp.bool), # type: ignore
                #     output: wp.array(dtype=int) # type: ignore
                # ) -> None:
                #     i = wp.tid()
                #     if premWater[i]:
                #         output[i] = 0
                #     else:
                #         output[i] = data[i]

                data: np.ndarray = dataset.read(mainBand)
                if mainBand != 5:
                    premWater: np.ndarray = dataset.read(5)
                    # Calculate with CPU
                    data = data * (~premWater.astype("bool"))
                    # # Calculate with GPU
                    # dataWp = wp.array(data, dtype=int)
                    # premWaterWp = wp.array(premWater, dtype=bool)
                    # outputWp = wp.zeros_like(dataWp)
                    # wp.launch(removePW, dim=data.size, inputs=[dataWp, premWaterWp, outputWp])
                    # data = np.array(outputWp)

                    data = np.where(data == 0, np.nan, data) # Change 0 to NaN
                
                
                meta["count"] = 1
                meta["nodata"] = 0
                ## Save masked raster into memory with meta information # Counsum too much memory and has overflow risk
                # rasterData = MemoryFile().open(**meta)
                # rasterData.write(data, 1)
                # Save masked raster into disk
                meta["compress"] = "DEFLATE"
                meta["zlevel"] = 9
                meta["predictor"] = 2 # Flot using 3
                meta["num_threads"] = "all_cpus"
                rasterData = os.path.join(path, "tmp", "{}.tif".format(tif))
                with rio.open(rasterData, 'w', **meta) as dst:
                    dst.write(data, 1)
                with datasLock:
                    datas.append(rasterData)
                
                # Release memory
                dataset.close()
                del data
                gc.collect()
            
            z.close()
            bar.update(1)
            return
        
        futures = []
        excutor = ThreadPoolExecutor(max_workers=multiThread)
        for file in files:
            futures.append(excutor.submit(readTif, datas, path, file, mainBand, bar, n))
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                tqdm.write("Error in read tif: {}".format(e))
        
        bar.set_description("Mosaicing all rasters to new raster")
        bar.update(1)
        datasets = [rio.open(fp) for fp in datas]
        bar.update(1)
        if mainBand == 5:
            mosaic, out_transform = merge(datasets, method="max", nodata=0) # Simiart to Mosaic To New Raster (Data Management) in ArcGIS
        else:
            mosaic, out_transform = merge(datasets, method="sum", nodata=0)
        bar.update(4)
        
        bar.set_description("Saving result")
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
        result = os.path.join(savePath, "{}_merge.tif".format(country))
        with rio.open(result, 'w', **outMeta) as dst:
            dst.write(mosaic)
        for ds in datasets:
            ds.close()

        # Save metadata
        metadata = pd.DataFrame({"File Names": files})
        metadata.to_csv(os.path.join(savePath, "{}_metadata.csv".format(country)), encoding="utf-8")
        bar.update(4)
        bar.close()
        shutil.rmtree(tmpPath)

        return result

if __name__ == "__main__":
    # Adjust the multi-thread number based on your computer, too much threads will cause memory overflow
    # floodingMerge(r"F:\\download", 512).mergeOneCountry("AGO", "test", 2)
    floodingMerge(r"C:\\0_PolyU\\flooding").mergeAll("test", 2, multiThread=5)