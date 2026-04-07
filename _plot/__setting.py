import matplotlib.pyplot as plt
from dataclasses import dataclass
from typing import TypedDict

# Fig size
LABEL_SIZE = 28
TICK_SIZE = int(LABEL_SIZE * 0.9)
NOTE_SIZE = int(LABEL_SIZE * 0.7)
@dataclass
class __FIG_SIZE:
    D: tuple[int, int] = (12, 12)   # Default
    DS: tuple[int, int] = (12, 9)
    D31: tuple[int, int] = (8, 8) # Default 1/3
    H: tuple[int, int] = (12, 24)   # High
    H31: tuple[int, int] = (12, 16) # Extra 1/3 High
    N: tuple[int, int] = (12, 6)   # Narrow
    W: tuple[int, int] = (24, 12)    # Wide
    W31: tuple[int, int] = (8, 12)    # Wide 1/3
    W32: tuple[int, int] = (16, 12)    # Wide 2/3
    WN31: tuple[int, int] = (24, 10)    # Wide Narrow
    WN32: tuple[int, int] = (24, 8)    # Wide Narrow 1/2
    S: tuple[int, int] = (6, 6)     # Small
    S2: tuple[int, int] = (8, 15) # Special
FIG_SIZE = __FIG_SIZE()

# Boxplot color kwgs
class __BoxplotKwargs(TypedDict):
    medianprops: dict[str, str]
    whiskerprops: dict[str, str]
    capprops: dict[str, str]
    meanprops: dict[str, str]

BOX_KWARGS: __BoxplotKwargs = {
    "medianprops": {"color": "white"},
    "whiskerprops": {"color": "gray"},
    "capprops": {"color": "gray"},
    "meanprops": {"markerfacecolor": "lightgreen"},
}