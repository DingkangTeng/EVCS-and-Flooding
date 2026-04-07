from dataclasses import dataclass
from typing import overload, Literal

# Columns
@dataclass
class A_POI:
    before: tuple = ("A_1Num", "A_2Num", "A_3Num", "A_POIAll")
    afterEVCS: tuple = tuple(["{}_After".format(i) for i in before])
    afterRoad: tuple = tuple(["{}_AfterG".format(i) for i in before])
    after: tuple = tuple(["{}_AfterG_After".format(i) for i in before])
    
@dataclass
class A_POP:
    before: tuple = ("A_young", "A_middle", "A_elderly", "A_Male", "A_Female", "A_All", "A_2024") # "A_children"
    afterEVCS: tuple = tuple(["{}_After".format(i) for i in before])
    afterRoad: tuple = tuple(["{}_AfterG".format(i) for i in before])
    after: tuple = tuple(["{}_AfterG_After".format(i) for i in before])
    staticBefore: tuple = ("A_young", "A_middle", "A_elderly", "A_Male", "A_Female", "A_All") # "A_children"
    staticAfterEVCS: tuple = tuple(["{}_After".format(i) for i in staticBefore])
    staticAfterRoad: tuple = tuple(["{}_AfterG".format(i) for i in staticBefore])
    staticAfter: tuple = tuple(["{}_AfterG_After".format(i) for i in staticBefore])
    dynamicBefore: tuple = ("A_All", "A_2024")
    dynamicAfterEVCS: tuple = ("A_All_After", "A_2024_After")
    dynamicAfterRoad: tuple = ("A_All_AfterG", "A_2024_AfterG")
    dynamicAfter: tuple = ("A_AfterG_After", "A_2024_AfterG_After")

@overload
def AColumns(analysisType: str, accOrEquity: str, level: Literal[3]) -> tuple[list[str], list[list[str]]]: ...

@overload
def AColumns(analysisType: str, accOrEquity: str, level: Literal[0, 1, 2] = 0) -> tuple[list[str], list[str]]: ...

def AColumns(analysisType: str, accOrEquity: str, level: int = 0) -> tuple[list[str], list[str]] | tuple[list[str], list[list[str]]]:
    '''
    Docstring for AColumns
    
    :param analysisType: Description
    :type analysisType: str
    :param accOrEquity: Description
    :type accOrEquity: str
    :param level: {0: all affected, 1: only EVCS affected by flooding, 2: only road affected by flooding, 3: (0, 1, 2)}
    :type level: int
    :return: Description
    :rtype: tuple[list[str], list[str]] | tuple[list[str], list[list[str]]]
    '''
    if analysisType == "POI":
        ABefore = list(A_POI.before)
        AAfterEVCS = list(A_POI.afterEVCS)
        AAfterRoad = list(A_POI.afterRoad)
        AAfter = list(A_POI.after)
    elif analysisType == "popStatic":
        ABefore = list(A_POP.staticBefore)
        AAfterEVCS = list(A_POP.staticAfterEVCS)
        AAfterRoad = list(A_POP.staticAfterRoad)
        AAfter = list(A_POP.staticAfter)
    elif analysisType == "popDynamic":
        ABefore = list(A_POP.dynamicBefore)
        AAfterEVCS = list(A_POP.dynamicAfterEVCS)
        AAfterRoad = list(A_POP.dynamicAfterRoad)
        AAfter = list(A_POP.dynamicAfter)
    elif analysisType == "pop":
        ABefore = list(A_POP.before)
        AAfterEVCS = list(A_POP.afterEVCS)
        AAfterRoad = list(A_POP.afterRoad)
        AAfter = list(A_POP.after)
    elif analysisType == "all":
        ABefore = list(A_POP.before) + list(A_POI.before)
        AAfterEVCS = list(A_POP.afterEVCS) + list(A_POI.afterEVCS)
        AAfterRoad = list(A_POP.afterRoad) + list(A_POI.afterRoad)
        AAfter = list(A_POP.after) + list(A_POI.after)
    else:
        raise RuntimeError("Unsupport analysis type {}.".format(analysisType))
    
    if accOrEquity == "equity":
        ABefore = ["{}_Gini".format(x) for x in ABefore]
        AAfterEVCS = ["{}_Gini".format(x) for x in AAfterEVCS]
        AAfterRoad = ["{}_Gini".format(x) for x in AAfterRoad]
        AAfter = ["{}_Gini".format(x) for x in AAfter]
    elif accOrEquity == "all":
        ABefore += ["{}_Gini".format(x) for x in ABefore]
        AAfterEVCS += ["{}_Gini".format(x) for x in AAfterEVCS]
        AAfterRoad += ["{}_Gini".format(x) for x in AAfterRoad]
        AAfter += ["{}_Gini".format(x) for x in AAfter]

    after = {
        0: AAfter,
        1: AAfterEVCS,
        2: AAfterRoad,
        3: (AAfter, AAfterEVCS, AAfterRoad)
    }

    return ABefore, after[level]

A_BEFORE = list(A_POI.before) + list(A_POP.before)
A_AFTER_EVCS = list(A_POI.afterEVCS) + list(A_POP.afterEVCS)
A_AFTER_ROAD = list(A_POI.afterRoad) + list(A_POP.afterRoad)
A_AFTER = list(A_POI.after) + list(A_POP.after)
A = A_BEFORE + A_AFTER_EVCS + A_AFTER_ROAD + A_AFTER

# Dict for relative population
__origionalDict = {
    "children": "population_All_children",
    "young": "population_All_young",
    "middle": "population_All_middle",
    "elderly": "population_All_elderly",
    "Male": "population_Male",
    "Female": "population_Female",
    "All": "population_All",
    "1Num": "POI_1Num",
    "2Num": "POI_2Num",
    "3Num": "POI_3Num",
    "2024": "otherRaster_landscan_global_2024",
    "POIAll": "POI_POIAll"
}
POP_DICT = {}
for a in A:
    POP_DICT[a] = __origionalDict[a.split('_')[1]]

# Plot standard name
STAND_NAME = {
    "A_children_change": "children",
    "A_young_change": "young",
    "A_middle_change": "middle-\nage",
    "A_elderly_change": "older",
    "A_Male_change": "male",
    "A_Female_change": "female",
    "A_All_change": "all",
    "A_2024_change": "dynamic",
    "A_1Num_change": "administrative\nand public",
    "A_2Num_change": "commercial\nand business ",
    "A_3Num_change": "lersure\nand tourism",
    "A_POIAll_change": "all POI",
    "A_children_Gini_change": "children",
    "A_young_Gini_change": "young",
    "A_middle_Gini_change": "middle-\nage",
    "A_elderly_Gini_change": "older",
    "A_Male_Gini_change": "male",
    "A_Female_Gini_change": "female",
    "A_All_Gini_change": "all",
    "A_2024_Gini_change": "dynamic",
    "A_1Num_Gini_change": "administrative\nand public",
    "A_2Num_Gini_change": "commercial\nand business ",
    "A_3Num_Gini_change": "lersure\nand tourism",
    "A_POIAll_Gini_change": "all POI"
}