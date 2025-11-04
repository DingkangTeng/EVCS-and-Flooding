from dataclasses import dataclass
@dataclass
class A_POI:
    before: tuple = ("A_1Num", "A_2Num", "A_3Num")
    after: tuple = tuple(["{}_After".format(i) for i in before])
@dataclass
class A_POP:
    before: tuple = ("A_children", "A_young", "A_middle", "A_elderly", "A_Male", "A_Female", "A_All")
    after: tuple = tuple(["{}_After".format(i) for i in before])

A_BEFORE = list(A_POI.before) + list(A_POP.before)
A_AFTER = list(A_POI.after) + list(A_POP.after)
A = A_BEFORE + A_AFTER

POP_DICT = {
    "A_children_After": "population_All_children",
    "A_children": "population_All_children",
    "A_young_After": "population_All_young",
    "A_young": "population_All_young",
    "A_middle_After": "population_All_middle",
    "A_middle": "population_All_middle",
    "A_elderly_After": "population_All_elderly",
    "A_elderly": "population_All_elderly",
    "A_Male_After": "population_Male",
    "A_Male": "population_Male",
    "A_Female_After": "population_Female",
    "A_Female": "population_Female",
    "A_All_After": "population_All",
    "A_All": "population_All",
    "A_1Num": "POI_1Num",
    "A_1Num_After": "POI_1Num",
    "A_2Num": "POI_2Num",
    "A_2Num_After": "POI_2Num",
    "A_3Num": "POI_3Num",
    "A_3Num_After": "POI_3Num",
    "": "otherRaster_landscan_global_2024"
}

from .mergeResult import mergeData
from .calUpperLevel import calUpperLevel
from .aggerateAnalysis import aggerateAnalysis
from .demograpicDiff import demograpicDiff