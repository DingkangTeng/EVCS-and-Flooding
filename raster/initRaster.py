import rasterio as rio

class initRaster:
    __slots__ = ["rasterPath", "crs", "bounds"]

    def __init__(self, rasterPath: str) -> None:
        self.rasterPath = rasterPath
        with rio.open(rasterPath) as src:
            self.crs = src.crs
            self.bounds = src.bounds

# Debug
if __name__ == "__main__":
    a = initRaster("D:\\flooding\\SumDays.tif")
    print(type(a.bounds))