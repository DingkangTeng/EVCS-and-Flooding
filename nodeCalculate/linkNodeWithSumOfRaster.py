import sys, sqlite3, os, time, psutil, gc, random, threading
import pandas as pd
import geopandas as gpd
import numpy as np
import rasterio as rio
from tqdm import tqdm
from scipy.spatial import KDTree
from concurrent.futures import as_completed, ThreadPoolExecutor
from rasterio.windows import Window
from rasterio.transform import xy
from rasterio.mask import mask

sys.path.append(".") # Set path to the roots

from _function.sqlite import spatialiteConnection, modifyTable, FID_INDEX
from _function.readFiles import readFiles, loadJsonRecord, mkdir
from _function.constant import GiB

class linkNodeWithSumOfRaster:
    __slots__ = ["BLOCK_SIZE_INIT", "THREAD_NUM", "lock"]

    def __init__(self, blockSize: int = 4096, maxThread: int = 1) -> None:
        self.BLOCK_SIZE_INIT = blockSize
        self.THREAD_NUM = maxThread
        self.lock = threading.Lock()

    @staticmethod
    def updateData(path: str, df: pd.DataFrame, fieldName: str) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # Add field
        cursor.addFields("nodes", (fieldName, "Real", 0, False))
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON nodes (fid)")
        conn.commit()
        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False, method="multi", chunksize=16383) #32766//2
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (nodesFid)")
        conn.commit()
        conn.execute("BEGIN TRANSACTION;")
        cursor.execute(
            f"""
            UPDATE nodes
            SET {fieldName} = tempTable.{fieldName}
                FROM tempTable
                WHERE tempTable.nodesFid = nodes.fid
            """
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()
        del df
        gc.collect()

        return
    
    @staticmethod
    def calOneChunk(
        rasterPath: str,
        tree: KDTree | None, nodeCount: int, ij: tuple[int, int], indices: np.intp | None,
        BLOCK_SIZE: int, width: int, height: int,
        maxDistance: int | None = None,
    ) -> tuple[np.ndarray, tuple[int, int], np.intp | None]:
        # Check memeory
        repeatTime = 0
        while True:
            if (psutil.virtual_memory().available > BLOCK_SIZE * BLOCK_SIZE * 32 \
                and psutil.virtual_memory().available > GiB) or \
                repeatTime > 3: # Check if memory is enough or exceed the repeat time
                    break
            else:
                repeatTime += 1
                time.sleep(random.randint(1, 5))
                gc.collect()

        i, j = ij
        # Read tif
        with rio.open(rasterPath, chunks=True, options=["NUM_THREADS=ALL_CPUS"]) as src:
            transform = src.transform
            colOff = i * BLOCK_SIZE
            rowOff = j * BLOCK_SIZE
            windowWidth = min(BLOCK_SIZE, width - colOff)
            windowHeight = min(BLOCK_SIZE, height - rowOff)
            window = Window(colOff, rowOff, windowWidth, windowHeight) # type: ignore
            chunk = src.read(1, window=window)

        # Read chunk
        rows, cols = np.indices(chunk.shape)
        flatChunk = chunk.ravel()
        del chunk, window
        gc.collect()

        # check indices cache
        if indices is None:
            if tree is None or rowOff is None or colOff is None:
                raise RuntimeError("tree, row, col is required when no indices cache.")
            # Calculates coordinate of pixels center
            globalRows = rowOff + rows
            globalCols = colOff + cols
            
            # Transform cols and rows index into coordinates
            x_coords, y_coords = xy(
                transform, 
                globalRows.ravel(), 
                globalCols.ravel()
            )
            coords = np.column_stack((x_coords, y_coords))
            del x_coords, y_coords
            gc.collect()
            
            # Query the nearest index
            if maxDistance is not None:
                distances, indices = tree.query(coords, distance_upper_bound=maxDistance) # I/O work can release GIL
                # Mark the indexs that exceeds the threshold
                overThresholdMask = (distances > maxDistance) | np.isinf(distances)
                indices[overThresholdMask] = -1 # type: ignore
            else:
                # No distance threshold
                _, indices = tree.query(coords)
            
        # Updates calculates results
        validMask = (indices != -1)
        
        if np.any(validMask):
            validIndices = indices[validMask] # type: ignore
            validValues = flatChunk[validMask]
            sums = np.bincount(
                validIndices, 
                weights=validValues,
                minlength=nodeCount
            )
            del validMask, validIndices, validValues
            gc.collect()
            return sums, ij, indices
        
        else:
            del validMask
            gc.collect()
            return np.zeros(nodeCount, dtype=np.float64), ij, indices
    
    # Read raster data in multi-thread/multi-process
    def readOneTif(
            self,
            tree: KDTree | None, dataNode: gpd.GeoDataFrame, fieldName: str, indicesDict: dict[tuple[int, int], np.intp],
            raster: str, BLOCK_SIZE: int = 4096,
            maxDistance: int | None = None,
            cache: bool = True
        ) -> tuple[np.ndarray | None, int | None]:

        # if fieldName in dataNode.columns:
        #     if dataNode[fieldName].sum() != 0:
        #         tqdm.write("{} has already been processed.".format(fieldName))
        #         return None, None
        
        pixelSums = np.zeros(dataNode.shape[0], dtype=np.float64)
        name = os.path.basename(raster)

        with rio.open(raster, options=["NUM_THREADS=ALL_CPUS"]) as src:
            rasterCrs = src.crs
            width, height = src.width, src.height
            # Calculat chunks
            nChunksX = int(np.ceil(width / BLOCK_SIZE))
            nChunksY = int(np.ceil(height / BLOCK_SIZE))
            # Transform node again if crs is different, normally do not need
            if dataNode.crs != rasterCrs:
                dataNode.to_crs(rasterCrs, inplace=True)
                node = np.array(list(zip(dataNode.geometry.x, dataNode.geometry.y)))
                tree = KDTree(node)
                indices = None
        
        totalChunk = nChunksX * nChunksY
        bar = tqdm(total=totalChunk, desc="Processing {}".format(name), unit="chunks")
        counts = dataNode.shape[0]

        futures = []
        with ThreadPoolExecutor(max_workers=self.THREAD_NUM) as executor:
            for i in range(nChunksX):
                for j in range(nChunksY):                     
                    # Submit task
                    indices = indicesDict.get((i, j), None)
                    if indices is not None:
                        future = executor.submit(self.calOneChunk, raster, None, counts, (i, j), indices, BLOCK_SIZE, width, height)
                    else:
                        future = executor.submit(self.calOneChunk, raster, tree, counts, (i, j), indices, BLOCK_SIZE, width, height, maxDistance)
                    futures.append(future)

            for future in as_completed(futures):
                try:
                    sums, ij, indices = future.result()
                except Exception as e:
                    tqdm.write("Error: {}".format(e))
                    return None, None
                else:
                    pixelSums += sums
                    # Clean cache when memeory is less than 1 gib
                    with self.lock:
                        if psutil.virtual_memory().available < GiB:
                            tqdm.write("Out of memory, clean cache.")
                            indicesDict.clear()
                            time.sleep(0.1)
                            gc.collect()
                        # Only save caches when memory larger than 8 gib
                        if psutil.virtual_memory().available > BLOCK_SIZE * BLOCK_SIZE * 32 * self.THREAD_NUM and cache: # type: ignore
                            indicesDict[ij] = indices
                    futures.remove(future)
                    gc.collect()
                    bar.update(1)

        results = np.zeros([counts, 2], dtype=np.float32)
        results[:, 0] = np.arange(1, counts + 1, dtype=np.float32)
        results[:, 1] = pixelSums

        bar.set_description("Saving results for {}".format(name))
        bar.close()
        
        return results, totalChunk
    
    def processOneLayer(
        self,
        layerNode: tuple[str, str],
        rastersDict: dict[str, tuple[str, str]],
        processedRaster: list,
        cache: bool = True
    ) -> tuple[str, list]:
        # Read node layer
        path, layer = layerNode
        nodeName = os.path.basename(path)
        indicesDict = {}
        totalChunk = None
        if rastersDict == {}:
            return nodeName, processedRaster
        
        rasterSet = set(rastersDict.keys())
        if len(processedRaster) != 0:
            for i in processedRaster:
                rasterSet.discard(i)
            tqdm.write("The following rasters for \"{}\" have already been processed and skipped: \n{}".format(nodeName, processedRaster))
        if len(rasterSet) == 0:
            tqdm.write("{} have already been processed and skipped.".format(nodeName))
            return nodeName, processedRaster
        
        # Read one raster to get crs
        dataNode = gpd.read_file(path, layer=layer, encoding="utf-8")
        dropCol = [col for col in ["osmid", "osmid_original", "x", "y", "street_count", "cluster"] if col in dataNode.columns]
        dataNode.drop(columns=dropCol, inplace=True)
        key, value = next(iter(rastersDict.items()))
        with rio.open(os.path.join(value[0], key)) as src:
            rasterCrs = src.crs
            width, height = src.width, src.height
            if dataNode.crs != rasterCrs:
                dataNode.to_crs(rasterCrs, inplace=True)   

        # Build KD-Tree
        node = np.array(list(zip(dataNode.geometry.x, dataNode.geometry.y)))
        tree = KDTree(node)

        # Arrange block size with reamain memory and max thread
        BLOCK_SIZE = int(min(
            self.BLOCK_SIZE_INIT, # Default max value
            max(width / self.THREAD_NUM, height / self.THREAD_NUM, self.BLOCK_SIZE_INIT), # by raster widith or hight to maximize utilitze CPU thread
            ((psutil.virtual_memory().available - 2 * GiB) / 32 / self.THREAD_NUM) ** 0.5, # by simply count raster size
        ))
        if BLOCK_SIZE < 1024:
            tqdm.write(
                f"Large raster file with too many threads, the block size is setted as: {BLOCK_SIZE}, this may take a longer time for process."
            )

        for raster in rasterSet:
            if indicesDict != {} and len(indicesDict) == totalChunk:
                tree = None
                gc.collect()
                tqdm.write("All corresponding points have been cached, the tree is deleted to release memeory.")
            rasterRoot, fieldName = rastersDict[raster]
            rasterPath = os.path.join(rasterRoot, raster)
            results, totalChunk = self.readOneTif(tree, dataNode, fieldName, indicesDict, rasterPath, BLOCK_SIZE, cache=cache)
            if not results is None:
                self.updateData(path, pd.DataFrame(results, columns=["nodesFid", fieldName]), rastersDict[raster][1])
            processedRaster.append(os.path.basename(raster))
                
        return nodeName, processedRaster

    def processAll(self, pathGpke: str, tifRootPath: str, tifsFolderName: str, cache: bool = True) -> None:
        allGpkgs = readFiles(pathGpke).specificFile(suffix=["gpkg"])
        allGpkgs.sort()
        totalBar = tqdm(total=len(allGpkgs), desc="Processing countries", unit="country")
        # Update log
        log = loadJsonRecord(os.path.join(pathGpke, "log.json"), "populationRaster", {})
        
        for gpkg in allGpkgs:
            processedRaster = log.get(gpkg, [])
            countryName = gpkg.split('.')[0]
            path = os.path.join(pathGpke, gpkg)
            # get all tifs
            tifDict = {}
            for tifs in readFiles(tifRootPath).specificFloder(contains=[tifsFolderName]):
                tifsPath = os.path.join(tifRootPath, tifs)
                tif = readFiles(tifsPath).specificFile(suffix=["tif"], contains=[countryName])
                # No tif data
                if len(tif) == 0:
                    noData = "No corresponding tif file for {} in {}".format(countryName, tifsPath)
                    tqdm.write(noData)
                    processedRaster.append(noData)
                    continue

                # Process tif
                tif = tif[0]
                if tif in processedRaster: continue
                tifDict[tif] = (tifsPath, tifs) # tifs looks like population_All / population_All_children ...
            nodeName, processedRaster = self.processOneLayer((path, "nodes"), tifDict, processedRaster, cache)

            log.append({nodeName: processedRaster})
            log.save()
            totalBar.update(1)
        
        totalBar.close()

        return
    
    def processWithOneTif(self, pathGpke: str, tifPath: str, boundaryPath: str | tuple[str, str], countryCol: str) -> None:
        if isinstance(boundaryPath, str):
            boundary = gpd.read_file(boundaryPath, encoding="utf-8", usecols=[countryCol, "geometry"])
        else:
            boundary = gpd.read_file(boundaryPath[0], layer=boundaryPath[1], encoding="utf-8")
            boundary = boundary[[countryCol, "geometry"]]
        
        allGpkgs = readFiles(pathGpke).specificFile(suffix=["gpkg"])
        allGpkgs.sort()
        tifDir = os.path.dirname(tifPath)
        tmpDir = os.path.join(tifDir, "tmp")
        tif = "otherRaster_{}".format(os.path.basename(tifPath).split('.')[0].replace('-','_'))
        mkdir(tmpDir)

        totalBar = tqdm(total=len(allGpkgs), desc="Processing countries", unit="country")
        # Update log
        log = loadJsonRecord(os.path.join(pathGpke, "log.json"), tif, [])
        
        for gpkg in allGpkgs:
            if gpkg in log:
                totalBar.update()
                continue
            countryName = gpkg.split('.')[0]
            path = os.path.join(pathGpke, gpkg)
            # Creat tmp tif file
            tmpPath = os.path.join(tmpDir, tif)
            geoseries = boundary.loc[boundary[countryCol] == countryName]
            if len(geoseries) == 0: continue
            ## Check crs
            with rio.open(tifPath) as src:
                rasterCrs = src.crs
                if geoseries.crs != rasterCrs: geoseries.to_crs(rasterCrs, inplace=True)

            ## Clip
            bounds = geoseries.union_all() # if len(geoseries) > 1 else geoseries.iloc[0]
            with rio.open(tifPath, options=["NUM_THREADS=ALL_CPUS"]) as src:
                outImage, outTransform = mask(
                    src, 
                    [bounds.__geo_interface__], 
                    crop=True,
                    all_touched=True
                )
                outImage[outImage == src.nodata] = 0
                out_meta = src.meta.copy()
                out_meta.update({
                    "driver": "GTiff",
                    "height": outImage.shape[1],
                    "width": outImage.shape[2],
                    "transform": outTransform,
                    "nodata": 0
                })
                with rio.open(tmpPath, "w", options=["NUM_THREADS=ALL_CPUS"], **out_meta) as dest:
                    dest.write(outImage)

            self.processOneLayer((path, "nodes"), {tif: (tmpDir, tif)}, [], cache=False)
            
            os.remove(tmpPath)
            log.append(gpkg)
            log.save()
            totalBar.update(1)
        
        os.rmdir(tmpDir)
        totalBar.close()

        return

if __name__ == "__main__":
    # linkNodeWithSumOfRaster(10240, 16).processOneLayer(
    #     ("test//CHN.gpkg", "nodes"),
    #     {"test//pop_Nanjing.tif": "allPopulation"},
    #     os.cpu_count()  # type: ignore
    # )

    linkNodeWithSumOfRaster(maxThread=4, blockSize=8192).processAll(r"C:\\0_PolyU\\roadsGraph", r"D:\Population_Related\global_2025", r"population_")
    # rasterDict = {
    #     "JPN_allGender_[60, 65, 70, 75, 80]_merge.tif": (r"C:\0_PolyU\population_All_elderly", "population_All_elderly2"),
    #     "JPN_allGender_allAge_merge.tif": (r"C:\0_PolyU\population_All", "population_All"),
    #     "JPN_allGender_[25, 30, 35, 40]_merge.tif": (r"C:\0_PolyU\population_All_young", "population_All_young"),
    #     "JPN_allGender_[0, 1, 5, 10, 15, 20]_merge.tif": (r"C:\0_PolyU\population_All_children", "population_All_children"),
    #     "JPN_allGender_[45, 50, 55]_merge.tif": (r"C:\0_PolyU\population_All_middle", "population_All_middle2"),
    #     "JPN_['f']_allAge_merge.tif": (r"C:\0_PolyU\population_Female", "population_Female"),
    #     "JPN_['m']_allAge_merge.tif": (r"C:\0_PolyU\population_Male", "population_Male")
    #     }
    # linkNodeWithSumOfRaster(maxThread=16, blockSize=1500).processOneLayer((r"C:\\0_PolyU\\test\\JPN.gpkg", "nodes"), rasterDict, [])

    # No corresponding population:
    # IOT, ATF

    # linkNodeWithSumOfRaster(maxThread=16).processWithOneTif(
    #     r"C:\\0_PolyU\\roadsGraph",
    #     r"C:\\0_PolyU\\landscan-global-2024-assets\\landscan-global-2024.tif",
    #     (r"_GISAnalysis\\Dissertation.gdb", "GAUL_2024_L2"),
    #     "iso3_code"
    # )