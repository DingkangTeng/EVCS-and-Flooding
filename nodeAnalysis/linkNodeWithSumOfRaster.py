import sys, gc, sqlite3, os
import pandas as pd
import geopandas as gpd
import numpy as np
import rasterio as rio
from tqdm import tqdm
from scipy.spatial import KDTree
from concurrent.futures import ThreadPoolExecutor, as_completed
from rasterio.windows import Window
from rasterio.transform import xy

sys.path.append(".") # Set path to the roots

from function.sqlite import spatialiteConnection, modifyTable
from function.readFiles import readFiles

class linkNodeWithSumOfRaster:
    def __init__(self) -> None:
        pass

    @staticmethod
    def updateData(path: str, df: pd.DataFrame, fieldName: str) -> None:
        conn = sqlite3.connect(path, factory=spatialiteConnection)
        conn.loadSpatialite() # Load spatialite extension
        cursor = conn.cursor(factory=modifyTable)
        # Add field
        cursor.addFields("nodes", (fieldName, "Real", 0))
        # Add data
        df.to_sql("tempTable", conn, if_exists="replace", index=False)
        cursor.execute(
            """
            UPDATE nodes
            SET {} = (SELECT tempTable.{}
                        FROM tempTable 
                        WHERE tempTable.nodesFid = nodes.fid),
            WHERE {} = 0
            """.format(
                fieldName,
                fieldName,
                fieldName
            )
        )
        cursor.execute("DROP TABLE IF EXISTS tempTable")
        conn.commit()
        conn.close()

        return
    
    def processOneLayer(self, layerNode: tuple[str, str], rasters: list[str], fieldName: str) -> None:
        # Read node layer
        path = layerNode[0]  # For updates data into gpkg
        dataNode = gpd.read_file(path, layer=layerNode[1], encoding="utf-8")
        pixelSums = np.zeros(dataNode.shape[0], dtype=np.float64)

        # Read one raster to get crs
        with rio.open(rasters[0]) as src:
            rasterCrs = src.crs
            if dataNode.crs != rasterCrs:
                dataNode = dataNode.to_crs(rasterCrs)

        # Build KD-Tree
        node = np.array(list(zip(dataNode.geometry.x, dataNode.geometry.y)))
        tree = KDTree(node)

        # Read raster data in multi-thread/multi-process
        def readOneTif(tree: KDTree, dataNode: gpd.GeoDataFrame, pixelSumsIn: np.ndarray, raster: str, maxDistance: int | None = None, chunkSize: int = 10240) -> None:
            pixelSums = pixelSumsIn.copy()
            with rio.open(raster, chunks=True, options=["NUM_THREADS=ALL_CPUS"]) as src:
                rasterCrs = src.crs
                width, height = src.width, src.height
                transform = src.transform
                # Calculat chunks
                nChunksX = int(np.ceil(width / chunkSize))
                nChunksY = int(np.ceil(height / chunkSize))
                totalChunks = nChunksX * nChunksY
                # Transform node again if crs is different, normally do not need
                if dataNode.crs != rasterCrs:
                    dataNode = dataNode.to_crs(rasterCrs)
                    node = np.array(list(zip(dataNode.geometry.x, dataNode.geometry.y)))
                    tree = KDTree(node)
                
                for i in range(nChunksX):
                    for j in range(nChunksY):
                        colOff = i * chunkSize
                        rowOff = j * chunkSize
                        windowWidth = min(chunkSize, width - colOff)
                        windowHeight = min(chunkSize, height - rowOff)
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
                            indices[overThresholdMask] = -1
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
                        "fid": i + 1,
                        "pixel_sum": pixelSums[i],
                    })
                
                # Updates data into gpkg
                # Test
                pd.DataFrame(results).to_csv("test//b(with distance).csv", encoding="utf-8")
            
            return
        
        readOneTif(tree, dataNode, pixelSums, rasters[0], 1000)

        # self.updateData(path, pd.DataFrame(results), fieldName)

        return

if __name__ == "__main__":
    linkNodeWithSumOfRaster().processOneLayer(
        ("test//OSM_Nanjin_ThirdRoad.gpkg", "nodes"),
        # ["test//Flooding_Nanjin2.tif"],
        ["C:\\0_PolyU\\population_All\\CHN_allGender_allAge_merge.tif"],
        "allPopulation"
    )

    # 10240*10240: 23G consume