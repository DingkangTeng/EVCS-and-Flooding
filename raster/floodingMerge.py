import sys, os, zipfile, threading, psutil, gc, shutil, time
import rasterio as rio
import pandas as pd
import numpy as np
# import warp as wp
from rasterio.io import MemoryFile
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from osgeo import gdal
from datetime import datetime

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, mkdir

class floodingMerge:
    __slots__ = ["path", "subThreadSize", "__maxThread"]

    def __init__(self, path: str, subThreadSize: int = 512):
        """
        Initialization setting

        Parameters:
        path: The root path of flooding tif file
        subThreadSize: Arrange how much memeory will cost in each sub-thread, \
        the unit is MB. (Default: `512`)

        Retruns:
        None
        """
        # Init gdal
        gdal.UseExceptions()
        gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")
        # 在代码开头配置GDAL缓存
        gdal.SetConfigOption("GDAL_CACHEMAX", "1024")  # 1024MB
        gdal.SetConfigOption("VSI_CACHE", "TRUE")
        gdal.SetConfigOption("VSI_CACHE_SIZE", "536870912")  # 512MB
        ## Init GPU
        # wp.init()
        self.path = path
        CPUCount = os.cpu_count()
        if CPUCount is None:
            CPUCount = 1
        memorySize = psutil.virtual_memory().available / (1024 ** 2) # One thread needs 512M in default
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
        def subThread(country: str, i: int, n: int, saved: list[str], savedLock: threading.Lock) -> None:
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
            future = excutor.submit(subThread, country, i, n, saved, savedLock)
            futures.append(future)
            futuresToCountry[future] = country
            i += 1
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                tqdm.write("Error in merge country {}: {}".format(futuresToCountry[future], e))

        # Different countries may have overlap，so use max (union)
        tqdm.write("Merging all countries...")
        mosaic = gdal.Warp(
            destNameOrDestDS=os.path.join(savePath, "0_All_merge.tif"),
            srcDSOrSrcDSTab=saved,
            format="GTiff",
            srcSRS="EPSG:4326", # Set to WGS84
            srcNodata=0,
            dstSRS="EPSG:4326", # Set to WGS84
            dstNodata=0,
            resampleAlg=gdal.GRA_Max,
            multithread=True,
            creationOptions=[
                "NUM_THREADS=ALL_CPUS",
                "COMPRESS=DEFLATE",
                "TILED=YES"
                ]
        )
        mosaic = None # Clean data
        del mosaic
        gc.collect()
        
        return

    def readAllTifInZip(self, savePath: str, mainBand: int, multiThread: int = 0) -> None:
        countries = readFiles(self.path).allFolder()
        c = len(countries)
        i = 0
        datas = set()
        # Get basic information
        for country in countries:
            path = os.path.join(self.path, country)
            files = readFiles(path).specifcFile(suffix=["zip"])
            n = len(files)
            i += n
        for file in readFiles(savePath).specifcFile(suffix=["tif"]):
            datas.add(file[0:-4])
        bar = tqdm(total=i, desc="Starting", postfix="Total {} countries".format(c))
        if multiThread == 0:
            multiThread = self.__maxThread

        futures = []
        futuresToCountry = {} # Store futures to country mapping for debugging
        excutor = ThreadPoolExecutor(max_workers=multiThread)
        for country in countries:
            path = os.path.join(self.path, country)
            files = readFiles(path).specifcFile(suffix=["zip"])
            n = len(files)
            
            if n == 0:
                tqdm.write("No tif files found in {}".format(country))
                continue

            for file in files:
                zipPath = os.path.join(path, file)
                z = zipfile.ZipFile(zipPath, 'r')
                for tif in z.namelist():
                    if tif.split('.')[-1] != "tif":
                        continue
                    elif tif in datas: # Avoid duplicate tif files
                        bar.set_description("Tif file {} already exists in datas and skipped".format(tif))
                        bar.update(1)
                    else:
                        z2 = zipfile.ZipFile(zipPath, 'r')
                        future = excutor.submit(self.readTifInZip, tif, z2, savePath, mainBand, bar, n, multiThread)
                        futures.append(future)
                        # Add debugging information
                        futuresToCountry[future] = country
                        datas.add(tif)
                z.close()

        for future in as_completed(futures):
            country = futuresToCountry[future]
            try:
                future.result()
            except Exception as e:
                tqdm.write("Error in merge country {}: {}".format(country, e))

        return

    def mergeOneCountry(self, country: str, savePath: str, mainBand: int, multiThread: int = 0) -> str | None:
        path = os.path.join(self.path, country)
        tmpPath = os.path.join(path, "tmp")
        result = os.path.join(savePath, "{}_merge.tif".format(country))
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
        
        futures = []
        excutor = ThreadPoolExecutor(max_workers=multiThread)
        for file in files:
            bar.set_description("Removing permanent water bodies ({} files)".format(n))
            zipPath = os.path.join(path, file)
            z = zipfile.ZipFile(zipPath, 'r')
            tifs = [x for x in z.namelist() if x.split('.')[-1] == "tif"]
            savePath = os.path.join(path, "tmp")
            rasterData = [os.path.join(savePath, "{}.tif".format(tif)) for tif in tifs]
            datas.append(rasterData)
            for tif in tifs:
                z2 = zipfile.ZipFile(zipPath, 'r')
                futures.append(excutor.submit(self.readTifInZip, tif, z2, savePath, mainBand, bar, n, multiThread))
            z.close()
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                tqdm.write("Error in read tif: {}".format(e))
        
        bar.set_description("Mosaicing all rasters to new raster")
        bar.update(2)
        if mainBand in [1, 5]:
            resample = gdal.GRA_Max
        else:
            resample = gdal.GRA_Sum
        mosaic = gdal.Warp(
            destNameOrDestDS=result,
            srcDSOrSrcDSTab=datas,
            format="GTiff",
            srcSRS="EPSG:4326", # Set to WGS84
            srcNodata=0,
            dstSRS="EPSG:4326", # Set to WGS84
            dstNodata=0,
            resampleAlg=resample,
            multithread=True,
            creationOptions=[
                "NUM_THREADS=ALL_CPUS",
                "COMPRESS=DEFLATE",
                "TILED=YES"
                ]
        )
        mosaic = None # Clean data
        del mosaic
        gc.collect()
        bar.set_description("Saving result")
        bar.update(4)

        # Save metadata
        metadata = pd.DataFrame({"File Names": files})
        metadata.to_csv(os.path.join(savePath, "{}_metadata.csv".format(country)), encoding="utf-8")
        bar.update(4)
        bar.close()
        shutil.rmtree(tmpPath)

        return result
    
    def readTifInZip(self, tif: str, z: zipfile.ZipFile, savePath: str, mainBand: int, bar: tqdm, n: int, multiThread: int) -> None:
        fileSize = z.getinfo(tif).file_size
        while True:
            if fileSize < psutil.virtual_memory().available // multiThread: # Check if memory is enough
                bar.set_description("Removing permanent water bodies ({} files)".format(n))
                break
            else:
                bar.set_description("Not enough memory of {}MB to read {}. Waiting   ".format(fileSize // (1024 ** 2), tif))
                time.sleep(0.1)
                bar.set_description("Not enough memory of {}MB to read {}. Waiting.  ".format(fileSize // (1024 ** 2), tif))
                time.sleep(0.1)
                bar.set_description("Not enough memory of {}MB to read {}. Waiting.. ".format(fileSize // (1024 ** 2), tif))
                time.sleep(0.1)
                bar.set_description("Not enough memory of {}MB to read {}. Waiting...".format(fileSize // (1024 ** 2), tif))
                gc.collect()
                threading.Event().wait(1)
        flooding = z.read(tif)
        dataset = MemoryFile(flooding).open() # Read tif file in memory
        meta = dataset.meta
        """
        Useful band
        Band 1: flooded
        Band 2: flood_duration
        Band 5: jrc_perm_water (1 - permanent water, 0 - non-water)
        """
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
            # Exclude permanent water
            ## mask = ~B5 # Change permanent water into 0, and no water into 1
            ## result = B1 * mask
            premWater: np.ndarray = dataset.read(5)
            # Calculate with CPU
            data = data * (~premWater.astype("bool"))
            # # Calculate with GPU
            # dataWp = wp.array(data, dtype=int)
            # premWaterWp = wp.array(premWater, dtype=bool)
            # outputWp = wp.zeros_like(dataWp)
            # wp.launch(removePW, dim=data.size, inputs=[dataWp, premWaterWp, outputWp])
            # data = np.array(outputWp)

            data = np.where(data == np.nan, 0, data) # Change NaN to 0, No data is 0
        
        
        meta["count"] = 1
        meta["nodata"] = 0
        ## Save masked raster into memory with meta information # Counsum too much memory and has overflow risk
        # rasterData = MemoryFile().open(**meta)
        # rasterData.write(data, 1)
        # Save masked raster into disk
        meta["compress"] = "DEFLATE"
        meta["zlevel"] = 9
        meta["predictor"] = 2 # Flot using 3
        meta["num_threads"] = "ALL_CPUS"

        rasterData = os.path.join(savePath, "{}.tif".format(tif))
        with rio.open(rasterData, 'w', **meta) as dst:
            dst.write(data, 1)
        
        # Release memory
        dataset.close()
        del data
        z.close()
        gc.collect()
        bar.update(1)

        return
    
    def calculateStasticPeriod(self, savePath: str) -> None:
        """
        Calculate the statistic period of flooding data
        """
        countries = readFiles(self.path).allFolder()
        results = {"Country": [], "StasticTimePeriod(dyas)": [], "StasticTimes": []}
        for country in countries:
            path = os.path.join(self.path, country)
            files = readFiles(path).specifcFile(suffix=["zip"])
            results["Country"].append(country)
            results["StasticTimes"].append(len(files))
            days = 0
            for file in files:
                standardName = file.split("_")
                startDay = datetime.strptime(standardName[3], "%Y%m%d")
                endDay = datetime.strptime(standardName[5][0:8], "%Y%m%d")
                days += ((endDay - startDay).days + 1) # Include the end day
            results["StasticTimePeriod(dyas)"].append(days)
        
        pd.DataFrame(results).to_csv(
            os.path.join(savePath, "stasticPeriod.csv"),
            index=False,
            encoding="utf-8"
        )

        return


if __name__ == "__main__":
    # Adjust the multi-thread number based on your computer, too much threads will cause memory overflow
    # Only remove water bodies
    floodingMerge(r"C:\\0_PolyU\\flooding2").readAllTifInZip("C:\\0_PolyU\\floodingAll_Days", 2, multiThread=os.cpu_count()) # type: ignore
    # floodingMerge(r"C:\\0_PolyU\\flooding").calculateStasticPeriod("C:\\0_PolyU\\flooding")
    ## gdal.Warp have some problems and will not merge all files correctly
    # floodingMerge(r"C:\\0_PolyU\\flooding").mergeOneCountry("CHN", "test", 2, multiThread=os.cpu_count()) # type: ignore
    # floodingMerge(r"C:\\0_PolyU\\flooding").mergeAll("C:\\0_PolyU\\floodingAll\\mergeByCountry", 2, multiThread=os.cpu_count() ** 0.5) # type: ignore