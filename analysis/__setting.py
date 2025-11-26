from dataclasses import dataclass
@dataclass
class A_POI:
    before: tuple = ("A_1Num", "A_2Num", "A_3Num", "A_POIAll")
    after: tuple = tuple(["{}_After".format(i) for i in before])
@dataclass
class A_POP:
    before: tuple = ("A_children", "A_young", "A_middle", "A_elderly", "A_Male", "A_Female", "A_All", "A_2024")
    after: tuple = tuple(["{}_After".format(i) for i in before])
    staticBefore: tuple = ("A_children", "A_young", "A_middle", "A_elderly", "A_Male", "A_Female", "A_All")
    staticAfter: tuple = tuple(["{}_After".format(i) for i in staticBefore])
    dynamicBefore: tuple = ("A_2024", "A_All")
    dynamicAfter: tuple = ("A_2024_After", "A_All_After")

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
    "A_2024": "otherRaster_landscan_global_2024",
    "A_2024_After": "otherRaster_landscan_global_2024",
    "A_POIAll": "POI_POIAll",
    "A_POIAll_After": "POI_POIAll"

}