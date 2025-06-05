import sys, os, zipfile, threading
import rasterio as rio
import pandas as pd
import numpy as np
from rasterio.io import MemoryFile
from rasterio.merge import merge
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, mkdir

class floodingMerge:
    __slot__ = ["path"]
    data = []

    def __init__(self, path: str):
        self.path = path
        
    def mergeAll(self, savePath: str, mainBand: int):
        countries = readFiles(self.path).allFolder()
        saved = [] # Save path for all countries
        n = len(countries)
        i = 1

        # A thread-safe way to append results
        savedLock = threading.Lock()
        def subThread(country: str, i: int, n: int, saved: list[str]) -> None:
            print("Processing {} ({}/{})".format(country, i, n))
            result = self.mergeOneCountry(country, savePath, mainBand)
            with savedLock:
                saved.append(result)

        futures = []
        excutor = ThreadPoolExecutor(max_workers=os.cpu_count())
        for country in countries:
            futures.append(excutor.submit(subThread, country, i, n, saved))
            i += 1
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print("Error processing a country: {}".format(e))

        # Different countries may have overlap，so use max (union)
        print("Merging all countries...")
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

    def mergeOneCountry(self, country: str, savePath: str, mainBand: int) -> str:
        path = os.path.join(self.path, country)
        mkdir(savePath)

        files = readFiles(path).specifcFile(suffix=["zip"])
        n = len(files)
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
                data: np.ndarray = dataset.read(mainBand)
                if mainBand != 5:
                    premWater: np.ndarray = dataset.read(5)
                    data = data * (~premWater.astype("bool"))
                    data = np.where(data == 0, np.nan, data) # Change 0 to NaN
                
                # Save masked raster into memory with meta information
                meta["count"] = 1
                meta["nodata"] = 0
                rasterData = MemoryFile().open(**meta)
                rasterData.write(data, 1)
                
                with datasLock:
                    datas.append(rasterData)
                dataset.close()
            
            z.close()
            bar.update(1)
        
        futures = []
        excutor = ThreadPoolExecutor(max_workers=os.cpu_count())
        for file in files:
            futures.append(excutor.submit(readTif, datas, path, file, mainBand, bar, n))
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print("Error processing a country: {}".format(e))
        
        bar.set_description("Mosaicing all rasters to new raster")
        bar.update(1)
        if mainBand in [1, 5]:
            mosaic, out_transform = merge(datas, method="max", nodata=0) # Simiart to Mosaic To New Raster (Data Management) in ArcGIS
        else:
            mosaic, out_transform = merge(datas, method="sum", nodata=0)
        bar.update(5)
        
        bar.set_description("Saving result")
        outMeta = datas[0].meta.copy()
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
        for ds in datas:
            ds.close()

        # Save metadata
        metadata = pd.DataFrame({"File Names": files})
        metadata.to_csv(os.path.join(savePath, "{}_metadata.csv".format(country)), encoding="utf-8")
        bar.update(4)
        bar.close()

        return result

if __name__ == "__main__":
    floodingMerge(r"F:\\Flooding").mergeOneCountry("ARE", "test", 2)
    # floodingMerge(r"F:\\Flooding").mergeAll("test", 2)