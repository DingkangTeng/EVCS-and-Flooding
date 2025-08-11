import sys, sqlite3, os, time, psutil
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
    __slots__ = ["BLOCK_SIZE"]

    def __init__(self, blockSize: int = 4096) -> None:
        self.BLOCK_SIZE = blockSize

    @staticmethod
    def updateData(path: str, df: pd.DataFrame, fieldName: str) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # Add field
        cursor.addFields("nodes", (fieldName, "Real", 0, True))
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON nodes (fid)")
        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {FID_INDEX} ON tempTable (fid)")
        conn.commit()
        cursor.execute(
            f"""
            UPDATE nodes
            SET {fieldName} = (SELECT tempTable.{fieldName}
                        FROM tempTable 
                        WHERE tempTable.nodesFid = nodes.fid)
            WHERE {fieldName} = 0
            """
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

        return
    
    # Read raster data in multi-thread/multi-process
    def readOneTif(
            self,
            tree: KDTree, dataNode: gpd.GeoDataFrame, fieldName: str,
            pixelSumsIn: np.ndarray, raster: str,
            maxDistance: int | None = None, blockSize: int = 4096
        ) -> list[dict]:
        pixelSums = pixelSumsIn.copy()
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
            
            for i in range(nChunksX):
                for j in range(nChunksY):
                    colOff = i * blockSize
                    rowOff = j * blockSize
                    windowWidth = min(blockSize, width - colOff)
                    windowHeight = min(blockSize, height - rowOff)
                    window = Window(colOff, rowOff, windowWidth, windowHeight) # type: ignore
                    chunk = src.read(1, window=window)
                    
                    # Calculates coordinate of pixels center
                    rows, cols = np.indices(chunk.shape)
                    global_rows = rowOff + rows
                    global_cols = colOff + cols
                    
                    # Transform cols and rows index into coordinates
                    x_coords, y_coords = xy(
                        transform, 
                        global_rows.ravel(), 
                        global_cols.ravel()
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
                    flatChunk = chunk.ravel()
                    for k in range(dataNode.shape[0]):
                        mask = (indices == k)
                        values = flatChunk[mask]
                        if values.size > 0:
                            pixelSums[k] += np.sum(values)
            
            results = []
            for i in range(dataNode.shape[0]):
                results.append({
                    "nodesFid": i + 1,
                    fieldName: pixelSums[i],
                })
        
        return results
    
    def processOneLayer(self, layerNode: tuple[str, str], rastersDict: dict[str, str], processedRaster: list, maxThread: int = 1) -> tuple[str, list]:
        # Read node layer
        path, layer = layerNode
        nodeName = os.path.basename(path)
        
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
        pixelSums = np.zeros(dataNode.shape[0], dtype=np.float64)
        with rio.open(list(rastersDict.keys())[0]) as src:
            rasterCrs = src.crs
            if dataNode.crs != rasterCrs:
                dataNode = dataNode.to_crs(rasterCrs)

        # Build KD-Tree
        node = np.array(list(zip(dataNode.geometry.x, dataNode.geometry.y)))
        tree = KDTree(node)
        
        futures = []
        debugDict = {}
        with ProcessPoolExecutor(max_workers=maxThread) as excutor:
            for raster in rasterSet:
                fileSize = os.path.getsize(raster)
                while True:
                    if fileSize < psutil.virtual_memory().available \
                        or psutil.virtual_memory().available > self.BLOCK_SIZE * self.BLOCK_SIZE * 256: # Check if memory is enough
                            break
                    else:
                        time.sleep(10)
                future = excutor.submit(self.readOneTif, tree, dataNode, rastersDict[raster], pixelSums, raster)
                debugDict[future] = raster
                futures.append(future)
            
            for future in as_completed(futures):
                raster = debugDict[future]
                try:
                    results = future.result()
                    self.updateData(path, pd.DataFrame(results), rastersDict[raster])
                except Exception as e:
                    tqdm.write("Error in processing {}: {}".format(raster, e))
                else:
                    processedRaster.append(raster)

        return nodeName, processedRaster

    def processAll(self, pathGpke: str, tifRootPath: str, tifsFolderName: str, maxThread: int = 1) -> None:
        allGpkgs = set(readFiles(pathGpke).specificFile(suffix=["gpkg"]))
        # Checking processed gpkg will be processed in slef.processOneLayer
        bar = tqdm(total = len(allGpkgs), desc="Running KD-Trees and calculating populations", unit="layer")
        # Update log
        log = loadJsonRecord(os.path.join(pathGpke, "log.json"), "populationRaster", {})
        
        futures = []
        debugDict = {}
        with ProcessPoolExecutor(max_workers=maxThread) as excutor:
            for gpkg in allGpkgs:
                processedRaster = log.get(gpkg, [])
                countryName = gpkg.split('.')[0]
                path = os.path.join(pathGpke, gpkg)
                # get all tifs
                tifDict = {}
                for tifs in readFiles(tifRootPath).specificFloder(contains=[tifsFolderName]):
                    tifsPath = os.path.join(tifRootPath, tifs)
                    tif = readFiles(tifsPath).specificFile(suffix=["tif"], contains=[countryName])
                    if len(tif) == 0:
                        raise RuntimeError("No corresponding tif file for {} in {}".format(countryName, tifsPath))
                    else:
                        tif = tif[0]
                    tifDict[os.path.join(tifsPath, tif)] = tifs # tifs looks like population_All / population_All_children ...
                future = excutor.submit(self.processOneLayer, (path, "nodes"), tifDict, processedRaster, maxThread)
                futures.append(future)
                debugDict[future] = "{}: {}".format(countryName)
                
            for future in as_completed(futures):
                countryName = debugDict[future]
                try:
                    nodeName, processedRaster = future.result()
                except Exception as e:
                    tqdm.write("Error in processing {}.gpkg: {}".format(countryName, e))
                else:
                    bar.update(1)
                    log.append({nodeName: processedRaster})
        
        log.save()
        bar.close()

        return

if __name__ == "__main__":
    # linkNodeWithSumOfRaster(10240).processOneLayer(
    #     ("test//CHN.gpkg", "nodes"),
    #     {"test//pop_Nanjing.tif": "allPopulation"},
    #     os.cpu_count()  # type: ignore
    # )

    linkNodeWithSumOfRaster().processAll(r"C:\\0_PolyU\\roadsGraph", r"C:\\0_PolyU", r"population_", os.cpu_count() ** 0.5) # type: ignore