import sys, os, threading, gc, shutil, psutil, time
import pandas as pd
import numpy as np
# try:
#     import cupy as np
# except Exception as e:
#     print(e, "Use CPU instead.")
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from osgeo import gdal

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles, mkdir
from raster.floodingMerge import floodingMerge
from function.gdalFunction import gdalDatasets

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
        bar = tqdm(total=n, desc="Starting")
        i = 1
        if multiThread == 0:
            multiThread = self.maxThread

        # A thread-safe way to append results
        savedLock = threading.Lock()
        def subThread(
            country: str, savePath:str, mainAge: list[int], gender: list[str], bar: tqdm, # Pass agrs
            i: int, n: int, # GUI
            saved: list[str], savedLock:threading.Lock # Save
        ) -> None:
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
        existDatas = set()
        for file in readFiles(savePath).specifcFile(suffix=["tif"]):
            existDatas.add(file.split('_')[0])
        for country in countries:
            if country in existDatas:
                tqdm.write("Country {}({}/{}) already exists in datas and skipped".format(country, i, n))
                i += 1
                bar.update(1)
                continue
            else:
                future = excutor.submit(subThread, country, savePath, mainAge, gender, bar, i, n, saved, savedLock)
                futures.append(future)
                futuresToCountry[future] = country
                i += 1
        for future in as_completed(futures):
            country = futuresToCountry[future]
            try:
                future.result()
            except Exception as e:
                tqdm.write("Error in merge country {}: {}".format(country, e))
        
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
        # Read all files by filter
        datas = []
        for file in files:
            attr = file.split("_")
            if attr[1] in gender and int(attr[2]) in mainAge:
                datas.append(file)
        if len(datas) == 0:
            tqdm.write("No tif files found in {}".format(country))
            return
        datasets = [os.path.join(path, fp) for fp in datas]
        

        # Check available memeory
        while True:
            totalSize = sum(os.path.getsize(d) for d in datasets)
            if totalSize // 2 < psutil.virtual_memory().available: # Check if memory is enough
                if bar is None:
                    tqdm.write("Mosacing {}".format(country))
                else:
                    bar.set_description("Mosacing {}".format(country))
                break
            else:
                if bar is None:
                    tqdm.write("Not enough memory of {}MB to read {}. Waiting   ".format(totalSize // (1024 ** 2), country))
                    time.sleep(0.1)
                    tqdm.write("Not enough memory of {}MB to read {}. Waiting.  ".format(totalSize // (1024 ** 2), country))
                    time.sleep(0.1)
                    tqdm.write("Not enough memory of {}MB to read {}. Waiting.. ".format(totalSize // (1024 ** 2), country))
                    time.sleep(0.1)
                    tqdm.write("Not enough memory of {}MB to read {}. Waiting...".format(totalSize // (1024 ** 2), country))
                    time.sleep(0.1)
                else:
                    bar.set_description("Not enough memory of {}MB to read {}. Waiting   ".format(totalSize // (1024 ** 2), country))
                    time.sleep(0.1)
                    bar.set_description("Not enough memory of {}MB to read {}. Waiting.  ".format(totalSize // (1024 ** 2), country))
                    time.sleep(0.1)
                    bar.set_description("Not enough memory of {}MB to read {}. Waiting.. ".format(totalSize // (1024 ** 2), country))
                    time.sleep(0.1)
                    bar.set_description("Not enough memory of {}MB to read {}. Waiting...".format(totalSize // (1024 ** 2), country))
                    time.sleep(0.1)
                gc.collect()
                threading.Event().wait(1)

        # Estimates of total number of people per grid square broken down by gender and age groupings, therefor use sum to merge
        result = os.path.join(
            savePath, "{}_{}_{}_merge.tif".format(
                country,
                gender if len(gender) == 1 else "allGender",
                mainAge if len(mainAge) != 18 else "allAge"
            )
        )
        # If only one raster, save directly
        if len(datasets) == 1:
            shutil.copyfile(datasets[0], result)
        else:
            # Get metadata
            with gdalDatasets(datasets[0]) as ds:
                XSize = ds.RasterXSize
                YSize = ds.RasterYSize
                trans = ds.GetGeoTransform()
                proj = ds.GetProjection()
            
            # Creat output data
            driver = gdal.GetDriverByName("GTiff")
            if not isinstance(driver, gdal.Driver):
                raise RuntimeError("Failed to creat driver.")
            outDs = driver.Create(
                result, XSize, YSize, 1, gdal.GDT_Float32,
                options=["COMPRESS=DEFLATE", "TILED=YES", "NUM_THREADS=ALL_CPUS", "BIGTIFF=IF_SAFER"]
            )
            if not isinstance(outDs, gdal.Dataset):
                raise RuntimeError("Failed to creat new dataset.")
            outDs.SetGeoTransform(trans)
            outDs.SetProjection(proj)
            outBand = outDs.GetRasterBand(1)
            if not isinstance(outBand, gdal.Band):
                raise RecursionError("Faild to creat new band.")
            outBand.SetNoDataValue(0)
            outBand.Fill(0)
            
            # Process with strip block
            for YOffset in range(0, YSize, self.BLOCK_SIZE):
                YBlock = min(self.BLOCK_SIZE, YSize - YOffset)
                blockShape = (YBlock, XSize)
                blockSum = np.zeros(blockShape, dtype=np.float32)

                for dataset in datasets:
                    with gdalDatasets(dataset) as ds:
                        band = ds.GetRasterBand(1)
                        if not isinstance(band, gdal.Band):
                            raise RecursionError("Faild to read band.")
                        arr = band.ReadAsArray(0, YOffset, XSize, YBlock)
                        if arr is None:
                            raise RecursionError("Faild to read band as array.")
                        
                        # Use GPU
                        if np.__name__ == "cupy":
                            arrGpu = np.asarray(arr)
                            arrGpu[arr == -99999] = 0  # Set nodata to 0 to avoid the disruption of sum
                            blockSum += arrGpu
                        else:
                            arr[arr == -99999] = 0
                            blockSum += arr
                
                if np.__name__ == "cupy":
                    outBand.WriteArray(np.asnumpy(blockSum), 0, YOffset) # type: ignore
                else:
                    outBand.WriteArray(blockSum, 0, YOffset)
                
                # Release memory
                outDs.FlushCache()
                del blockSum
                if np.__name__ == "cupy":
                    np.get_default_memory_pool().free_all_blocks() # type: ignore
                gc.collect()
        
            outDs.Destroy()

        # Save metadata
        metadata = pd.DataFrame({"File Names": datas})
        metadata.to_csv(os.path.join(savePath, "{}_metadata.csv".format(country)), encoding="utf-8")

        if bar is not None:
            bar.update(1)

        return result

if __name__ == "__main__":
    # Adjust the multi-thread number based on your computer, too much threads will cause memory overflow
    # populationMerge(r"C:\\0_PolyU\\population\\", blockSize=4096).mergeByAge("CHN", "test")
    populationMerge(r"C:\\0_PolyU\\population\\", blockSize=2048).mergeAll(r"C:\\0_PolyU\\population_All", multiThread=4) # type: ignore
    #需要重新下载人口的国家：IDN、IND、IRN、IRQ、ISR、JOR、JPN、KAZ、KGZ、KWT、LAO、LBN