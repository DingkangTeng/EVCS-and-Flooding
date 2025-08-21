import sys, sqlite3, os, time, psutil, gc
import pandas as pd
import geopandas as gpd
import numpy as np
import rasterio as rio
from tqdm import tqdm
from scipy.spatial import KDTree
from concurrent.futures import as_completed, ProcessPoolExecutor
from rasterio.windows import Window
from rasterio.transform import xy

sys.path.append(".") # Set path to the roots

from function.sqlite import spatialiteConnection, modifyTable, FID_INDEX
from function.readFiles import readFiles, loadJsonRecord

class linkNodeWithSumOfRaster:
    __slots__ = ["BLOCK_SIZE", "executor"]

    def __init__(self, blockSize: int = 4096, maxThread: int = 1) -> None:
        self.BLOCK_SIZE = blockSize
        self.executor = ProcessPoolExecutor(max_workers=maxThread)

    @staticmethod
    def updateData(path: str, df: pd.DataFrame, fieldName: str) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # cursor.execute("PRAGMA synchronous = WAL;")
        # cursor.execute("PRAGMA journal_mode = NORMAL;")
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

        return
    
    @staticmethod
    def calOneChunk(
        chunk,
        tree: KDTree | None, nodeCount: int, ij: tuple[int, int], indices: np.intp | None,
        rowOff: int | None = None, colOff: int | None = None,
        transform: str | None = None,
        maxDistance: int | None = None,
    ) -> tuple[np.ndarray, tuple[int, int], np.intp | None]:
        # read tif
        rows, cols = np.indices(chunk.shape)
        flatChunk = chunk.ravel()

        # check indices cache
        if indices is None:
            if tree is None or rowOff is None or colOff is None:
                raise RuntimeError("tree, row, col is required when no indices caches")
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
            
            # Query the nearest index
            if maxDistance is not None:
                distances, indices = tree.query(coords, distance_upper_bound=maxDistance)
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
            return sums, ij, indices
        else:
            return np.zeros(nodeCount, dtype=np.float64), ij, indices
    
    # Read raster data in multi-thread/multi-process
    def readOneTif(
            self,
            tree: KDTree, dataNode: gpd.GeoDataFrame, fieldName: str, indicesDict: dict[tuple[int, int], np.intp],
            raster: str,
            maxDistance: int | None = None, blockSize: int = 4096
        ) -> list[dict]:

        if fieldName in dataNode.columns:
            if dataNode[fieldName].sum() != 0:
                tqdm.write("{} has already been processed.".format(fieldName))
                return []
        
        pixelSums = np.zeros(dataNode.shape[0], dtype=np.float64)
        fileSize = os.path.getsize(raster)
        name = os.path.basename(raster)

        with rio.open(raster, chunks=True, options=["NUM_THREADS=ALL_CPUS"]) as src:
            rasterCrs = src.crs
            width, height = src.width, src.height
            transform = src.transform
            # Calculat chunks
            nChunksX = int(np.ceil(width / blockSize))
            nChunksY = int(np.ceil(height / blockSize))
            # Transform node again if crs is different, normally do not need
            if dataNode.crs != rasterCrs:
                dataNode = dataNode.to_crs(rasterCrs)
                node = np.array(list(zip(dataNode.geometry.x, dataNode.geometry.y)))
                tree = KDTree(node)
                indices = None
            
            bar = tqdm(total=nChunksX*nChunksY, desc="Processing {}".format(name), unit="chunks")
            counts = dataNode.shape[0]

            futures = []
            for i in range(nChunksX):
                for j in range(nChunksY):
                    # Check memeory
                    while True:
                        if fileSize < psutil.virtual_memory().available \
                            or psutil.virtual_memory().available > self.BLOCK_SIZE * self.BLOCK_SIZE * 256: # Check if memory is enough
                                bar.set_description("Processing {}".format(name))
                                break
                        else:
                            bar.set_description("Not enough memory, waiting...")
                            gc.collect()
                            time.sleep(10)
                    # Read chunk
                    colOff = i * blockSize
                    rowOff = j * blockSize
                    windowWidth = min(blockSize, width - colOff)
                    windowHeight = min(blockSize, height - rowOff)
                    window = Window(colOff, rowOff, windowWidth, windowHeight) # type: ignore
                    chunk = src.read(1, window=window)
                    # Submit task
                    indices = indicesDict.get((i, j), None)
                    if indices is not None:
                        future = self.executor.submit(self.calOneChunk, chunk, None, counts, (i, j), indices)
                    else:
                        future = self.executor.submit(self.calOneChunk, chunk, tree, counts, (i, j), indices, rowOff, colOff, transform, maxDistance)
                    futures.append(future)

            for future in as_completed(futures):
                try:
                    sums, ij, indices = future.result()
                except Exception as e:
                    tqdm.write("Error: {}".format(e))
                    return []
                else:
                    pixelSums += sums
                    indicesDict[ij] = indices
                    bar.update(1)
            
            results = [
                {
                    "nodesFid": i + 1,
                    fieldName: pixelSums[i],
                } for i in range(counts)
            ]

            bar.set_description("Saving results for {}".format(name))
            bar.close()
        
        return results
    
    def processOneLayer(self, layerNode: tuple[str, str], rastersDict: dict[str, tuple[str, str]], processedRaster: list) -> tuple[str, list]:
        # Read node layer
        path, layer = layerNode
        nodeName = os.path.basename(path)
        indicesDict = {}
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
        key, value = next(iter(rastersDict.items()))
        with rio.open(os.path.join(value[0], key)) as src:
            rasterCrs = src.crs
            if dataNode.crs != rasterCrs:
                dataNode = dataNode.to_crs(rasterCrs)

        # Build KD-Tree
        node = np.array(list(zip(dataNode.geometry.x, dataNode.geometry.y)))
        tree = KDTree(node)
        
        for raster in rasterSet:
            rasterRoot, fieldName = rastersDict[raster]
            rasterPath = os.path.join(rasterRoot, raster)
            results = self.readOneTif(tree, dataNode, fieldName, indicesDict, rasterPath)
            if results != []:
                self.updateData(path, pd.DataFrame(results), rastersDict[raster][1])
            processedRaster.append(os.path.basename(raster))
                
        return nodeName, processedRaster

    def processAll(self, pathGpke: str, tifRootPath: str, tifsFolderName: str) -> None:
        allGpkgs = readFiles(pathGpke).specificFile(suffix=["gpkg"])
        # Update log
        log = loadJsonRecord(os.path.join(pathGpke, "log.json"), "populationRaster", {})
        
        for gpkg in allGpkgs:
            # while True:
            #     if psutil.virtual_memory().available > 1024 ** 3: # Check if memory is enough
            #         break
            #     else:
            #         bar.set_description("Waiting for memory to be enough...")
            #         time.sleep(10)
            processedRaster = log.get(gpkg, [])
            countryName = gpkg.split('.')[0]
            path = os.path.join(pathGpke, gpkg)
            # get all tifs
            tifDict = {}
            for tifs in readFiles(tifRootPath).specificFloder(contains=[tifsFolderName]):
                tifsPath = os.path.join(tifRootPath, tifs)
                tif = readFiles(tifsPath).specificFile(suffix=["tif"], contains=[countryName])
                if len(tif) == 0:
                    tqdm.write("No corresponding tif file for {} in {}".format(countryName, tifsPath))
                    break
                tif = tif[0]
                tifDict[tif] = (tifsPath, tifs) # tifs looks like population_All / population_All_children ...
            nodeName, processedRaster = self.processOneLayer((path, "nodes"), tifDict, processedRaster)

            log.append({nodeName: processedRaster})
            log.save()

        return

if __name__ == "__main__":
    # linkNodeWithSumOfRaster(10240, 16).processOneLayer(
    #     ("test//CHN.gpkg", "nodes"),
    #     {"test//pop_Nanjing.tif": "allPopulation"},
    #     os.cpu_count()  # type: ignore
    # )

    linkNodeWithSumOfRaster(maxThread=16).processAll(r"C:\\0_PolyU\\roadsGraphtest", r"C:\\0_PolyU", r"population_") # type: ignore
    # rasterDict = {
    #     "CZE_allGender_[60, 65, 70, 75, 80]_merge.tif": (r"C:\0_PolyU\population_All_elderly", "population_All_elderly"),
    #     "CZE_allGender_allAge_merge.tif": (r"C:\0_PolyU\population_All", "population_All"),
    #     "CZE_allGender_[25, 30, 35, 40]_merge.tif": (r"C:\0_PolyU\population_All_young", "population_All_young"),
    #     "CZE_allGender_[0, 1, 5, 10, 15, 20]_merge.tif": (r"C:\0_PolyU\population_All_children", "population_All_children"),
    #     "CZE_allGender_[45, 50, 55]_merge.tif": (r"C:\0_PolyU\population_All_middle", "population_All_middle2"),
    #     "CZE_['f']_allAge_merge.tif": (r"C:\0_PolyU\population_Female", "population_Female"),
    #     "CZE_['m']_allAge_merge.tif": (r"C:\0_PolyU\population_Male", "population_Male")
    #     }
    # linkNodeWithSumOfRaster(maxThread=16).processOneLayer((r"C:\\0_PolyU\\roadsGraph\\CZE.gpkg", "nodes"), rasterDict, [])