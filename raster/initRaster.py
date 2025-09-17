import rasterio as rio
from rasterio.coords import BoundingBox

class initRaster:
    __slots__ = ["rasterPath", "_crs", "_bounds"]

    def __init__(self, rasterPath: str) -> None:
        self.rasterPath = rasterPath
        with rio.open(rasterPath) as src:
            self._crs = src.crs
            self._bounds = src.bounds
    
    @property
    def crs(self):
        return self._crs
    
    @property
    def bounds(self) -> BoundingBox:
        return self._bounds

# Debug
if __name__ == "__main__":
    a = initRaster("D:\\flooding\\SumDays.tif")
    print(type(a.bounds))