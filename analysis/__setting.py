from dataclasses import dataclass

# Columns
@dataclass
class A_POI:
    before: tuple = ("A_1Num", "A_2Num", "A_3Num", "A_POIAll")
    after: tuple = tuple(["{}_After".format(i) for i in before])
    
@dataclass
class A_POP:
    before: tuple = ("A_young", "A_middle", "A_elderly", "A_Male", "A_Female", "A_All", "A_2024") # "A_children"
    after: tuple = tuple(["{}_After".format(i) for i in before])
    staticBefore: tuple = ("A_young", "A_middle", "A_elderly", "A_Male", "A_Female", "A_All") # "A_children"
    staticAfter: tuple = tuple(["{}_After".format(i) for i in staticBefore])
    dynamicBefore: tuple = ("A_All", "A_2024")
    dynamicAfter: tuple = ("A_All_After", "A_2024_After")

def AColumns(analysisType: str, accOrEquity: str) -> tuple[list[str], list[str]]:
    if analysisType == "POI":
        ABefore = list(A_POI.before)
        AAfter = list(A_POI.after)
    elif analysisType == "popStatic":
        ABefore = list(A_POP.staticBefore)
        AAfter = list(A_POP.staticAfter)
    elif analysisType == "popDynamic":
        ABefore = list(A_POP.dynamicBefore)
        AAfter = list(A_POP.dynamicAfter)
    else:
        raise RuntimeError("Unsupport analysis type {}.".format(analysisType))
    
    if accOrEquity == "equity":
        ABefore = ["{}_Gini".format(x) for x in ABefore]
        AAfter = ["{}_Gini".format(x) for x in AAfter]

    return ABefore, AAfter

A_BEFORE = list(A_POI.before) + list(A_POP.before)
A_AFTER = list(A_POI.after) + list(A_POP.after)
A = A_BEFORE + A_AFTER

# Dict for relative population
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

# Plot standard name
STAND_NAME = {
    "A_children_changeRatio": "children",
    "A_young_changeRatio": "young\nadult",
    "A_middle_changeRatio": "middle-age\nadult",
    "A_elderly_changeRatio": "older adult",
    "A_Male_changeRatio": "male",
    "A_Female_changeRatio": "female",
    "A_All_changeRatio": "all\npopulation",
    "A_2024_changeRatio": "dynamic\npopulation",
    "A_1Num_changeRatio": "administrative\nand public",
    "A_2Num_changeRatio": "commercial\nand business ",
    "A_3Num_changeRatio": "lersure\nand tourism",
    "A_poiall_changeRatio": "all\nPOI",
    "A_children_Gini_changeRatio": "children",
    "A_young_Gini_changeRatio": "young\nadult",
    "A_middle_Gini_changeRatio": "middle-age\nadult",
    "A_elderly_Gini_changeRatio": "older adult",
    "A_Male_Gini_changeRatio": "male",
    "A_Female_Gini_changeRatio": "female",
    "A_All_Gini_changeRatio": "all\npopulation",
    "A_2024_Gini_changeRatio": "dynamic\npopulation",
    "A_1Num_Gini_changeRatio": "administrative\nand public",
    "A_2Num_Gini_changeRatio": "commercial\nand business ",
    "A_3Num_Gini_changeRatio": "lersure\nand tourism",
    "A_poiall_Gini_changeRatio": "all\nPOI"
}