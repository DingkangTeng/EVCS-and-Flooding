# Rectangle extent
class extent:
    def __init__(self, xmin: float, ymin: float, xmax: float, ymax: float):
        self.XMin = xmin
        self.YMin = ymin
        self.XMax = xmax
        self.YMax = ymax

    def __str__(self):
        return "Extent: (%s, %s, %s, %s)" % (self.XMin, self.YMin,
                                              self.XMax, self.YMax)